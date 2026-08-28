"""PySpark parallel occupancy/revenue computation for AVR-Flow.

AVR-Flow:
A Partitioned Parallel and Distributed Occupancy Revenue Intelligence
Platform for Dynamic Pricing Models in Short-Term Rental Hospitality.

Dataset:
    Seattle Airbnb Open Data

Core files:
    listings.csv
    calendar.csv
    reviews.csv

Main parallel workload:
    calendar -> listings
    partition key: listing_id

Occupancy proxy:
    available == 'f'

Revenue proxy:
    price for occupied/unavailable nights

Important:
    reviews.csv is intentionally NOT joined row-by-row with calendar.csv.
    A row-wise calendar x reviews join would create a many-to-many fan-out
    and could inflate revenue calculations.
"""

import json
import sys
import time

import pandas as pd

import config as cfg


# ======================================================================
# LOAD SPARK DATA
# ======================================================================

def load_spark_frames(spark):
    """Load calendar and listings without Spark schema inference."""

    from pyspark.sql import functions as F

    def read_csv(path):
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(str(path))
        )

    calendar = read_csv(
        cfg.path_for("calendar")
    )

    listings = read_csv(
        cfg.path_for("listings")
    )

    # --------------------------------------------------------------
    # Required columns
    # --------------------------------------------------------------

    required_calendar = {
        cfg.PARTITION_KEY,
        cfg.EVENT_TIME_FIELD,
        cfg.PRICE_FIELD,
        cfg.AVAILABILITY_FIELD,
    }

    required_listings = {
        cfg.LISTING_PRIMARY_KEY,
    }

    missing_calendar = (
        required_calendar
        - set(calendar.columns)
    )

    missing_listings = (
        required_listings
        - set(listings.columns)
    )

    if missing_calendar:
        raise ValueError(
            "calendar.csv is missing required columns: "
            + ", ".join(sorted(missing_calendar))
        )

    if missing_listings:
        raise ValueError(
            "listings.csv is missing required columns: "
            + ", ".join(sorted(missing_listings))
        )

    # --------------------------------------------------------------
    # Normalize join keys to STRING
    # --------------------------------------------------------------

    calendar = calendar.withColumn(
        cfg.PARTITION_KEY,
        F.trim(
            F.col(
                cfg.PARTITION_KEY
            ).cast("string")
        )
    )

    listings = listings.withColumn(
        cfg.LISTING_PRIMARY_KEY,
        F.trim(
            F.col(
                cfg.LISTING_PRIMARY_KEY
            ).cast("string")
        )
    )

    # Remove empty/null listing IDs.

    calendar = calendar.filter(
        F.col(
            cfg.PARTITION_KEY
        ).isNotNull()
        &
        (
            F.col(
                cfg.PARTITION_KEY
            ) != ""
        )
    )

    listings = listings.filter(
        F.col(
            cfg.LISTING_PRIMARY_KEY
        ).isNotNull()
        &
        (
            F.col(
                cfg.LISTING_PRIMARY_KEY
            ) != ""
        )
    )

    return calendar, listings


# ======================================================================
# BUILD JOINED EVENT DATASET
# ======================================================================

def build_joined(spark, verbose=True):

    from pyspark.sql import functions as F

    calendar, listings = load_spark_frames(
        spark
    )

    # ==============================================================
    # DATE
    # ==============================================================

    calendar = calendar.withColumn(
        cfg.EVENT_TIME_FIELD,
        F.to_date(
            F.col(
                cfg.EVENT_TIME_FIELD
            ),
            "yyyy-MM-dd"
        )
    )

    # ==============================================================
    # PRICE
    # ==============================================================

    # The Seattle Airbnb calendar price can contain currency symbols
    # and comma separators.
    #
    # Examples:
    #     $85.00
    #     $1,250.00
    #     85.00
    #
    # Convert it explicitly to DOUBLE.

    calendar = calendar.withColumn(
        "_price_clean",
        F.regexp_replace(
            F.col(
                cfg.PRICE_FIELD
            ).cast("string"),
            r"[$,\s]",
            ""
        )
    )

    calendar = calendar.withColumn(
        "_price",
        F.col(
            "_price_clean"
        ).cast("double")
    )

    # ==============================================================
    # AVAILABILITY
    # ==============================================================

    calendar = calendar.withColumn(
        "_available_normalized",
        F.lower(
            F.trim(
                F.col(
                    cfg.AVAILABILITY_FIELD
                ).cast("string")
            )
        )
    )

    # According to the Seattle Airbnb calendar convention:
    #
    #     t = available
    #     f = unavailable
    #
    # We use unavailable as the occupancy proxy.

    calendar = calendar.withColumn(
        "occupied_proxy",
        F.col(
            "_available_normalized"
        )
        ==
        F.lit(
            cfg.BOOKED_VALUE
        )
    )

    # ==============================================================
    # REVENUE
    # ==============================================================

    calendar = calendar.withColumn(
        "estimated_booked_revenue",
        F.when(
            F.col("occupied_proxy")
            &
            F.col("_price").isNotNull(),
            F.col("_price")
        )
        .otherwise(
            F.lit(0.0)
        )
    )

    # ==============================================================
    # LISTING ENTITY
    # ==============================================================

    candidate_listing_columns = [
        cfg.LISTING_PRIMARY_KEY,
        "property_type",
        "room_type",
        "neighbourhood",
        "accommodates",
        "bedrooms",
        "beds",
        "bathrooms",
        "review_scores_rating",
        "number_of_reviews",
    ]

    listing_cols = [
        column
        for column in candidate_listing_columns
        if column in listings.columns
    ]

    listings = listings.select(
        *listing_cols
    )

    listings = listings.dropDuplicates(
        [cfg.LISTING_PRIMARY_KEY]
    )

    # ==============================================================
    # ROW COUNT BEFORE JOIN
    # ==============================================================

    rows_before = calendar.count()

    # ==============================================================
    # JOIN
    # ==============================================================

    calendar_key = F.col(
        f"c.{cfg.PARTITION_KEY}"
    ).cast("string")

    listing_key = F.col(
        f"l.{cfg.LISTING_PRIMARY_KEY}"
    ).cast("string")

    joined = (
        calendar.alias("c")
        .join(
            F.broadcast(
                listings.alias("l")
            ),
            calendar_key == listing_key,
            "inner",
        )
        .drop(
            F.col(
                f"l.{cfg.LISTING_PRIMARY_KEY}"
            )
        )
    )

    # ==============================================================
    # ROW COUNT AFTER JOIN
    # ==============================================================

    rows_after = joined.count()

    # ==============================================================
    # DATA QUALITY DIAGNOSTICS
    # ==============================================================

    price_stats = joined.agg(
        F.count(
            F.col("_price")
        ).alias("valid_price_rows"),

        F.count(
            F.when(
                F.col("_price").isNull(),
                True
            )
        ).alias("null_price_rows"),

        F.count(
            F.when(
                F.col("occupied_proxy"),
                True
            )
        ).alias("occupied_rows"),

        F.count(
            F.when(
                F.col("occupied_proxy")
                &
                F.col("_price").isNotNull(),
                True
            )
        ).alias(
            "occupied_rows_with_price"
        ),
    ).collect()[0]

    # ==============================================================
    # SPARK EXECUTION PLAN
    # ==============================================================

    plan = (
        joined
        ._jdf
        .queryExecution()
        .executedPlan()
        .toString()
    )

    broadcast_hash_joins = plan.count(
        "BroadcastHashJoin"
    )

    sort_merge_joins = plan.count(
        "SortMergeJoin"
    )

    # ==============================================================
    # REPORT
    # ==============================================================

    report = {
        "calendar_rows_before": rows_before,
        "rows_after_listing_join": rows_after,
        "rows_dropped": (
            rows_before
            - rows_after
        ),
        "input_partitions":
            calendar.rdd.getNumPartitions(),

        "broadcast_hash_joins":
            broadcast_hash_joins,

        "sort_merge_joins":
            sort_merge_joins,

        "join_type":
            "many-to-one",

        "join_key":
            cfg.PARTITION_KEY,

        "calendar_key_type":
            "string",

        "listing_key_type":
            "string",

        "reviews_joined_rowwise":
            False,

        "valid_price_rows":
            int(
                price_stats[
                    "valid_price_rows"
                ]
            ),

        "null_price_rows":
            int(
                price_stats[
                    "null_price_rows"
                ]
            ),

        "occupied_rows":
            int(
                price_stats[
                    "occupied_rows"
                ]
            ),

        "occupied_rows_with_price":
            int(
                price_stats[
                    "occupied_rows_with_price"
                ]
            ),
    }

    # ==============================================================
    # DISPLAY
    # ==============================================================

    if verbose:

        cfg.banner(
            "SPARK CALENDAR + LISTING JOIN"
        )

        print(
            f"Rows before              : "
            f"{rows_before:,}"
        )

        print(
            f"Rows after               : "
            f"{rows_after:,}"
        )

        print(
            f"Rows dropped             : "
            f"{rows_before - rows_after:,}"
        )

        print(
            f"Input partitions         : "
            f"{calendar.rdd.getNumPartitions()}"
        )

        print(
            f"BroadcastHashJoin        : "
            f"{broadcast_hash_joins}"
        )

        print(
            f"SortMergeJoin            : "
            f"{sort_merge_joins}"
        )

        print(
            f"Valid price rows         : "
            f"{price_stats['valid_price_rows']:,}"
        )

        print(
            f"Null price rows          : "
            f"{price_stats['null_price_rows']:,}"
        )

        print(
            f"Occupied rows            : "
            f"{price_stats['occupied_rows']:,}"
        )

        print(
            f"Occupied rows with price : "
            f"{price_stats['occupied_rows_with_price']:,}"
        )

        print(
            "Join key                 : "
            f"{cfg.PARTITION_KEY}"
        )

        print(
            "Join key types           : "
            "STRING = STRING"
        )

        print(
            "Reviews joined           : "
            "NO (fan-out avoided)"
        )

    # ==============================================================
    # CACHE
    # ==============================================================

    joined.cache()

    joined.count()

    return joined, report


# ======================================================================
# PARALLEL AGGREGATION
# ======================================================================

def compute_parallel(
    joined,
    partitions
):

    from pyspark.sql import functions as F

    if partitions <= 0:
        raise ValueError(
            "Number of partitions must be greater than zero."
        )

    # ==============================================================
    # PARTITION BY LISTING_ID
    # ==============================================================

    partitioned = joined.repartition(
        partitions,
        cfg.PARTITION_KEY
    )

    # ==============================================================
    # AGGREGATE
    # ==============================================================

    result = (
        partitioned
        .groupBy(
            cfg.PARTITION_KEY
        )
        .agg(

            F.count(
                cfg.EVENT_TIME_FIELD
            ).alias(
                "observed_nights"
            ),

            F.sum(
                F.col(
                    "occupied_proxy"
                ).cast("long")
            ).alias(
                "occupied_nights"
            ),

            F.sum(
                "estimated_booked_revenue"
            ).alias(
                "estimated_revenue"
            ),

            F.avg(
                F.when(
                    F.col(
                        "occupied_proxy"
                    ),
                    F.col("_price")
                )
            ).alias(
                "average_booked_price"
            ),
        )

        # ==========================================================
        # OCCUPANCY RATE
        # ==========================================================

        .withColumn(
            "occupancy_rate",
            F.when(
                F.col(
                    "observed_nights"
                ) > 0,
                F.col(
                    "occupied_nights"
                )
                /
                F.col(
                    "observed_nights"
                )
            )
            .otherwise(
                F.lit(0.0)
            )
        )

        .select(
            cfg.PARTITION_KEY,
            "observed_nights",
            "occupied_nights",
            "occupancy_rate",
            "estimated_revenue",
            "average_booked_price",
        )
    )

    return partitioned, result


# ======================================================================
# VALIDATION
# ======================================================================

def validate(
    parallel_pd,
    baseline_pd,
    verbose=True
):
    """
    Validate Spark parallel output against pandas baseline.

    listing_id is normalized to STRING in both DataFrames so that
    validation is independent of pandas' automatic dtype inference.
    """

    numeric = [
        "observed_nights",
        "occupied_nights",
        "occupancy_rate",
        "estimated_revenue",
        "average_booked_price",
    ]

    # ==============================================================
    # REQUIRED COLUMNS
    # ==============================================================

    required = {
        cfg.PARTITION_KEY,
        *numeric,
    }

    missing_parallel = (
        required
        - set(parallel_pd.columns)
    )

    missing_baseline = (
        required
        - set(baseline_pd.columns)
    )

    if missing_parallel:
        raise ValueError(
            "Parallel result is missing columns: "
            + ", ".join(
                sorted(missing_parallel)
            )
        )

    if missing_baseline:
        raise ValueError(
            "Baseline result is missing columns: "
            + ", ".join(
                sorted(missing_baseline)
            )
        )

    # ==============================================================
    # NORMALIZE JOIN KEY
    # ==============================================================

    parallel_pd = parallel_pd.copy()

    baseline_pd = baseline_pd.copy()

    parallel_pd[
        cfg.PARTITION_KEY
    ] = (
        parallel_pd[
            cfg.PARTITION_KEY
        ]
        .astype("string")
        .str.strip()
    )

    baseline_pd[
        cfg.PARTITION_KEY
    ] = (
        baseline_pd[
            cfg.PARTITION_KEY
        ]
        .astype("string")
        .str.strip()
    )

    # ==============================================================
    # NORMALIZE NUMERIC COLUMNS
    # ==============================================================

    for column in numeric:

        parallel_pd[column] = pd.to_numeric(
            parallel_pd[column],
            errors="coerce"
        )

        baseline_pd[column] = pd.to_numeric(
            baseline_pd[column],
            errors="coerce"
        )

    # ==============================================================
    # SORT
    # ==============================================================

    p = (
        parallel_pd
        .sort_values(
            cfg.PARTITION_KEY
        )
        .reset_index(drop=True)
    )

    b = (
        baseline_pd
        .sort_values(
            cfg.PARTITION_KEY
        )
        .reset_index(drop=True)
    )

    # ==============================================================
    # MERGE
    # ==============================================================

    merged = p.merge(
        b,
        on=cfg.PARTITION_KEY,
        suffixes=(
            "_par",
            "_base"
        ),
        how="outer",
        indicator=True,
    )

    keys_match = bool(
        (
            merged["_merge"]
            == "both"
        ).all()
    )

    # ==============================================================
    # DIFFERENCES
    # ==============================================================

    diffs = {}

    for column in numeric:

        parallel_values = pd.to_numeric(
            merged[
                f"{column}_par"
            ],
            errors="coerce"
        )

        baseline_values = pd.to_numeric(
            merged[
                f"{column}_base"
            ],
            errors="coerce"
        )

        # Both NaN = equivalent.
        both_nan = (
            parallel_values.isna()
            &
            baseline_values.isna()
        )

        difference = (
            parallel_values
            - baseline_values
        ).abs()

        difference = difference.mask(
            both_nan,
            0.0
        )

        difference = difference.fillna(
            float("inf")
        )

        diffs[column] = float(
            difference.max()
        )

    # ==============================================================
    # PASS/FAIL
    # ==============================================================

    passed = (
        len(p) == len(b)
        and keys_match

        # Exact integer measures.
        and diffs[
            "observed_nights"
        ] == 0

        and diffs[
            "occupied_nights"
        ] == 0

        # Floating-point measures.
        and all(
            value < cfg.TOLERANCE
            for key, value in diffs.items()
            if key not in (
                "observed_nights",
                "occupied_nights",
            )
        )
    )

    report = {
        "parallel_groups":
            len(p),

        "baseline_groups":
            len(b),

        "partition_keys_match":
            keys_match,

        "max_differences":
            diffs,

        "tolerance":
            cfg.TOLERANCE,

        "passed":
            bool(passed),
    }

    # ==============================================================
    # DISPLAY
    # ==============================================================

    if verbose:

        print(
            "\nCorrectness:",
            "PASSED"
            if passed
            else "FAILED"
        )

        print(
            json.dumps(
                report,
                indent=2
            )
        )

    if not passed:

        raise AssertionError(
            "Parallel output does not match "
            "sequential baseline."
        )

    return report


# ======================================================================
# MAIN
# ======================================================================

def main():

    from sequential_baseline import (
        run_baseline
    )

    cfg.banner(
        "AVR-FLOW - PARALLEL COMPUTE"
    )

    spark = cfg.build_spark()

    try:

        # ==========================================================
        # STEP 1
        # ==========================================================

        joined, join_report = build_joined(
            spark
        )

        # ==========================================================
        # STEP 2
        # ==========================================================

        start = time.perf_counter()

        partitioned, result = (
            compute_parallel(
                joined,
                cfg.CHOSEN_PARTITIONS
            )
        )

        groups = result.count()

        elapsed = (
            time.perf_counter()
            - start
        )

        # ==========================================================
        # PERFORMANCE
        # ==========================================================

        print(
            f"\nConfigured partitions : "
            f"{partitioned.rdd.getNumPartitions()}"
        )

        print(
            f"Result groups         : "
            f"{groups:,}"
        )

        print(
            f"Execution time        : "
            f"{elapsed:.4f} s"
        )

        # ==========================================================
        # TOP REVENUE
        # ==========================================================

        print(
            "\nTop 10 listings by "
            "estimated revenue:"
        )

        (
            result
            .orderBy(
                "estimated_revenue",
                ascending=False
            )
            .show(
                10,
                truncate=False
            )
        )

        # ==========================================================
        # TO PANDAS
        # ==========================================================

        parallel_pd = (
            result
            .toPandas()
        )

        # ==========================================================
        # BASELINE
        # ==========================================================

        if cfg.OUT_BASELINE.exists():

            baseline_pd = pd.read_csv(
                cfg.OUT_BASELINE
            )

        else:

            print(
                "\nSequential baseline "
                "not found."
            )

            print(
                "Running sequential "
                "baseline..."
            )

            baseline_pd, _ = (
                run_baseline(
                    verbose=False
                )
            )

        # ==========================================================
        # VALIDATION
        # ==========================================================

        validation = validate(
            parallel_pd,
            baseline_pd
        )

        # ==========================================================
        # SAVE FINAL RESULT
        # ==========================================================

        parallel_pd.to_parquet(
            cfg.OUT_FINAL,
            index=False
        )

        # ==========================================================
        # SAVE VALIDATION
        # ==========================================================

        validation_payload = {

            "join":
                join_report,

            "aggregation": {

                "partitions":
                    cfg.CHOSEN_PARTITIONS,

                "groups":
                    groups,

                "seconds":
                    round(
                        elapsed,
                        4
                    ),
            },

            "validation":
                validation,
        }

        cfg.OUT_VALIDATION.write_text(
            json.dumps(
                validation_payload,
                indent=2
            ),
            encoding="utf-8"
        )

        # ==========================================================
        # SUCCESS
        # ==========================================================

        print(
            f"\nWrote "
            f"{cfg.OUT_FINAL}"
        )

        print(
            f"Wrote "
            f"{cfg.OUT_VALIDATION}"
        )

        print(
            "\nAVR-FLOW parallel "
            "computation completed "
            "successfully."
        )

    finally:

        spark.stop()

    return 0


# ======================================================================
# ENTRY POINT
# ======================================================================

if __name__ == "__main__":
    sys.exit(main())