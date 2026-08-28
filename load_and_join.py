"""Build AVR-Flow's calendar working set without review fan-out."""
import sys
import time
import pandas as pd
import config as cfg

def load_frames():
    return {name: pd.read_csv(cfg.path_for(name)) for name in cfg.FILES}

def _clean_price(series):
    return pd.to_numeric(
        series.astype(str).str.replace("$","",regex=False)
        .str.replace(",","",regex=False), errors="coerce")

def build_working_dataset(frames=None, verbose=True):
    if frames is None:
        frames = load_frames()
    calendar = frames["calendar"].copy()
    listings = frames["listings"].copy()

    required = [cfg.PARTITION_KEY,cfg.EVENT_TIME_FIELD,
                cfg.AVAILABILITY_FIELD,cfg.PRICE_FIELD]
    missing = [c for c in required if c not in calendar.columns]
    if missing:
        raise KeyError(f"calendar.csv missing required columns: {missing}")
    if cfg.LISTING_PRIMARY_KEY not in listings.columns:
        raise KeyError("listings.csv is missing primary key column 'id'")

    calendar[cfg.EVENT_TIME_FIELD] = pd.to_datetime(
        calendar[cfg.EVENT_TIME_FIELD], errors="coerce")
    calendar[cfg.PRICE_FIELD] = _clean_price(calendar[cfg.PRICE_FIELD])
    calendar["occupied_proxy"] = (
        calendar[cfg.AVAILABILITY_FIELD].astype(str).str.lower().eq(cfg.BOOKED_VALUE))
    calendar["estimated_booked_revenue"] = calendar[cfg.PRICE_FIELD].where(
        calendar["occupied_proxy"],0.0)

    listing_cols = [cfg.LISTING_PRIMARY_KEY] + [
        c for c in ("property_type","room_type","neighbourhood","accommodates",
                    "bedrooms","beds","bathrooms","review_scores_rating",
                    "number_of_reviews") if c in listings.columns]
    listing_dim = listings[listing_cols].drop_duplicates(cfg.LISTING_PRIMARY_KEY)

    start = time.perf_counter()
    working = calendar.merge(
        listing_dim,
        left_on=cfg.PARTITION_KEY,
        right_on=cfg.LISTING_PRIMARY_KEY,
        how="inner", validate="many_to_one")
    elapsed = time.perf_counter()-start

    report = {
        "calendar_rows_before":len(calendar),
        "calendar_listing_rows_after":len(working),
        "rows_dropped":len(calendar)-len(working),
        "join_seconds":round(elapsed,4),
        "join_path":"calendar |> listings (many-to-one)",
        "reviews_joined_rowwise":False,
        "review_fanout_avoided":True,
    }
    if verbose:
        cfg.banner("AVR-FLOW WORKING DATASET")
        print(f"Calendar rows before : {len(calendar):,}")
        print(f"Rows after join      : {len(working):,}")
        print(f"Rows dropped         : {report['rows_dropped']:,}")
        print(f"Join time            : {report['join_seconds']} s")
        print("Reviews              : aggregated separately; no row-wise fan-out")
    return working, report

def aggregate_reviews(frames=None):
    if frames is None:
        frames = load_frames()
    reviews = frames["reviews"].copy()
    if cfg.PARTITION_KEY not in reviews.columns:
        raise KeyError("reviews.csv is missing listing_id")
    agg = {"review_count":(cfg.PARTITION_KEY,"size")}
    if "id" in reviews.columns:
        agg["review_count"] = ("id","count")
    if cfg.EVENT_TIME_FIELD in reviews.columns:
        agg["first_review_date"]=(cfg.EVENT_TIME_FIELD,"min")
        agg["last_review_date"]=(cfg.EVENT_TIME_FIELD,"max")
    return reviews.groupby(cfg.PARTITION_KEY).agg(**agg).reset_index()

def main():
    cfg.banner("AVR-FLOW - LOAD AND JOIN")
    frames=load_frames()
    for name,df in frames.items():
        print(f"loaded {name:<12} {len(df):>10,} rows")
    working,_=build_working_dataset(frames)
    working.to_parquet(cfg.OUT_JOINED,index=False)
    print(f"\nWrote {cfg.OUT_JOINED} ({len(working):,} rows)")
    counts=working[cfg.PARTITION_KEY].value_counts()
    cfg.banner(f"PARTITION KEY PREVIEW - {cfg.PARTITION_KEY}")
    print(f"distinct values : {counts.size}")
    print(f"records/key     : min={counts.min()} median={int(counts.median())} max={counts.max()}")
    if counts.min()>0:
        print(f"skew ratio      : {counts.max()/counts.min():.2f} : 1")
    return 0

if __name__ == "__main__":
    sys.exit(main())
