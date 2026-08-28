"""Shared configuration for AVR-Flow (MIT 261)."""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "Datasets"
RESULTS_DIR = REPO_ROOT / "results"
DOCS_DIR = REPO_ROOT / "docs"
ARCH_DIR = REPO_ROOT / "architecture"
for d in (RESULTS_DIR, DOCS_DIR, ARCH_DIR):
    d.mkdir(parents=True, exist_ok=True)

FILES = {
    "calendar": {"file": "calendar.csv", "role": "Event"},
    "listings": {"file": "listings.csv", "role": "Entity"},
    "reviews": {"file": "reviews.csv", "role": "Event"},
}

PARTITION_KEY = "listing_id"
LISTING_PRIMARY_KEY = "id"
EVENT_TIME_FIELD = "date"
AVAILABILITY_FIELD = "available"
PRICE_FIELD = "price"
BOOKED_VALUE = "f"

PARTITION_SETTINGS = (2, 4, 8)
BENCHMARK_REPEATS = 3
BASELINE_REPEATS = 5
CHOSEN_PARTITIONS = 4
TOLERANCE = 1e-6

SPARK_APP_NAME = "MIT261-AVR-Flow"
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "2g"
SPARK_SHUFFLE_PARTITIONS = "8"
BROADCAST_FILES = ("listings",)

OUT_PROFILE = RESULTS_DIR / "file_profile.json"
OUT_JOINED = RESULTS_DIR / "calendar_listing_working.parquet"
OUT_BASELINE = RESULTS_DIR / "baseline_occupancy_revenue.csv"
OUT_BENCHMARK = RESULTS_DIR / "avr_flow_benchmark.csv"
OUT_PARTITIONS = RESULTS_DIR / "partition_sizes.csv"
OUT_PARTITION_STRATEGY = RESULTS_DIR / "partition_strategy.json"
OUT_FINAL = RESULTS_DIR / "occupancy_revenue_intelligence.parquet"
OUT_VALIDATION = RESULTS_DIR / "validation_report.json"

def path_for(name: str) -> Path:
    return DATA_DIR / FILES[name]["file"]

def build_spark():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
