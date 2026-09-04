"""
AVR-FLOW - POSTGRESQL DATA LOADER

Loads the Seattle Airbnb Open Data CSV files into PostgreSQL.

Expected project structure:

Session2-AVR-Flow/
│
├── load_postgres.py
│
└── Datasets/
    ├── listings.csv
    ├── calendar.csv
    └── reviews.csv

Tables created:

    listings
    calendar
    reviews

The loader automatically resolves paths relative to this
Python script, so it works regardless of the current
terminal working directory.

Requirements:

    pip install pandas psycopg2-binary
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

import pandas as pd

# Pylance / pyright may report this as missing unless the PostgreSQL driver
# is installed in the active Python environment. The app handles that at
# runtime and tells the user to install psycopg2-binary when needed.
# pyright: reportMissingImports=false
try:
    import psycopg2  # type: ignore[import-not-found,reportMissingImports]
    from psycopg2 import sql
except ImportError as exc:
    raise RuntimeError(
        "Missing PostgreSQL driver. Install it with: pip install psycopg2-binary"
    ) from exc


# ============================================================
# PROJECT PATHS
# ============================================================

# Folder containing load_postgres.py
BASE_DIR = Path(__file__).resolve().parent

# Dataset folder inside Session2-AVR-Flow
DATASETS_DIR = BASE_DIR / "Datasets"

# CSV files
LISTINGS_CSV = DATASETS_DIR / "listings.csv"
CALENDAR_CSV = DATASETS_DIR / "calendar.csv"
REVIEWS_CSV = DATASETS_DIR / "reviews.csv"


# ============================================================
# POSTGRESQL CONFIGURATION
# ============================================================

# IMPORTANT:
# Change these values to match your PostgreSQL setup.

DB_HOST = "localhost"
DB_PORT = 5432

DB_NAME = "Session2-AVR-Flow"
DB_USER = "postgres"
DB_PASSWORD = "root"


# ============================================================
# LOADING CONFIGURATION
# ============================================================

# Large CSV files are loaded in chunks.
CHUNK_SIZE = 50_000

# PostgreSQL schema
DB_SCHEMA = "public"

# Replace existing tables when loading again.
DROP_EXISTING_TABLES = True


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def banner(title: str) -> None:
    """Print a formatted section header."""

    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def check_files() -> None:
    """
    Verify that all required CSV files exist.

    The files must be located inside:

        Session2-AVR-Flow/Datasets/
    """

    banner("CHECKING DATASET FILES")

    print(f"Project directory:")
    print(f"{BASE_DIR}")

    print()
    print(f"Dataset directory:")
    print(f"{DATASETS_DIR}")

    print()

    required_files = {
        "listings.csv": LISTINGS_CSV,
        "calendar.csv": CALENDAR_CSV,
        "reviews.csv": REVIEWS_CSV,
    }

    for file_name, file_path in required_files.items():

        if not file_path.exists():
            raise FileNotFoundError(
                "\nCSV file not found.\n\n"
                f"File expected:\n{file_name}\n\n"
                f"Expected location:\n{file_path}\n\n"
                "Please make sure your project structure is:\n\n"
                "Session2-AVR-Flow/\n"
                "│\n"
                "├── load_postgres.py\n"
                "│\n"
                "└── Datasets/\n"
                f"    ├── listings.csv\n"
                f"    ├── calendar.csv\n"
                f"    └── reviews.csv\n"
            )

        print(f"[OK] {file_name}")
        print(f"     {file_path}")

    print()
    print("All required CSV files were found successfully.")


def get_connection():
    """Create and return a PostgreSQL connection."""

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def test_database_connection() -> None:
    """Test the PostgreSQL database connection."""

    banner("TESTING POSTGRESQL CONNECTION")

    connection = None

    try:
        connection = get_connection()

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    current_database(),
                    current_user,
                    version();
                """
            )

            result = cursor.fetchone()

            print(f"Database : {result[0]}")
            print(f"User     : {result[1]}")
            print()
            print("PostgreSQL connection successful.")

    finally:

        if connection is not None:
            connection.close()


def quote_identifier(name: str) -> str:
    """
    Return a safely quoted PostgreSQL identifier.

    This allows CSV column names to be used safely.
    """

    return '"' + name.replace('"', '""') + '"'


# ============================================================
# DATA TYPE DETECTION
# ============================================================

def postgres_type(series: pd.Series) -> str:
    """
    Infer a reasonable PostgreSQL data type from a pandas Series.

    The CSV files contain a mixture of:
        text
        integer values
        floating point values
        dates
        boolean-like values

    Conservative types are used to reduce loading failures.
    """

    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"

    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"

    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"

    return "TEXT"


def infer_table_columns(csv_path: Path) -> list[tuple[str, str]]:
    """
    Read a sample of the CSV and determine column names and
    PostgreSQL data types.
    """

    sample = pd.read_csv(
        csv_path,
        nrows=10_000,
        low_memory=False,
        encoding="utf-8",
    )

    columns = []

    for column in sample.columns:

        pg_type = postgres_type(sample[column])

        columns.append(
            (str(column), pg_type)
        )

    return columns


# ============================================================
# TABLE MANAGEMENT
# ============================================================

def create_table(
    connection,
    table_name: str,
    csv_path: Path,
) -> None:
    """
    Create a PostgreSQL table using the CSV header and
    inferred data types.
    """

    banner(f"CREATING TABLE: {table_name}")

    columns = infer_table_columns(csv_path)

    with connection.cursor() as cursor:

        if DROP_EXISTING_TABLES:

            print(f"Dropping existing table if present: {table_name}")

            cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                    sql.Identifier(DB_SCHEMA, table_name)
                )
            )

        column_definitions = []

        for column_name, column_type in columns:

            definition = sql.SQL("{} {}").format(
                sql.Identifier(column_name),
                sql.SQL(column_type),
            )

            column_definitions.append(definition)

        create_statement = sql.SQL(
            "CREATE TABLE {} ({})"
        ).format(
            sql.Identifier(DB_SCHEMA, table_name),
            sql.SQL(", ").join(column_definitions),
        )

        cursor.execute(create_statement)

    connection.commit()

    print(f"Table created successfully: {table_name}")

    print()
    print("Columns:")

    for column_name, column_type in columns:
        print(f"  {column_name} -> {column_type}")


# ============================================================
# CSV CLEANING
# ============================================================

def clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a pandas chunk before sending it to PostgreSQL.

    The goal is to:
        - preserve data
        - convert NaN to PostgreSQL NULL
        - keep column names unchanged
    """

    cleaned = chunk.copy()

    # Convert pandas NaN / NA to None.
    cleaned = cleaned.where(
        pd.notnull(cleaned),
        None,
    )

    return cleaned


# ============================================================
# BULK COPY
# ============================================================

def copy_dataframe_chunk(
    connection,
    dataframe: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Bulk load a pandas DataFrame chunk into PostgreSQL using COPY.

    COPY is much faster than inserting one row at a time.
    """

    dataframe = clean_chunk(dataframe)

    buffer = io.StringIO()

    dataframe.to_csv(
        buffer,
        index=False,
        header=False,
        sep="\t",
        na_rep="",
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
    )

    buffer.seek(0)

    columns = list(dataframe.columns)

    column_sql = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in columns
    )

    copy_statement = sql.SQL(
        """
        COPY {} ({})
        FROM STDIN
        WITH (
            FORMAT CSV,
            DELIMITER E'\\t',
            NULL '',
            QUOTE '"',
            ESCAPE '\\'
        )
        """
    ).format(
        sql.Identifier(DB_SCHEMA, table_name),
        column_sql,
    )

    with connection.cursor() as cursor:

        cursor.copy_expert(
            copy_statement.as_string(connection),
            buffer,
        )


# ============================================================
# LOAD CSV FILE
# ============================================================

def load_csv_to_postgres(
    connection,
    csv_path: Path,
    table_name: str,
) -> int:
    """
    Load a CSV file into PostgreSQL.

    The CSV is processed in chunks so that large files,
    especially calendar.csv, do not need to fit completely
    into memory.
    """

    banner(f"LOADING {csv_path.name} INTO TABLE: {table_name}")

    total_rows = 0
    chunk_number = 0

    print(f"Source file:")
    print(csv_path)

    print()
    print(f"Chunk size: {CHUNK_SIZE:,} rows")

    print()

    for chunk in pd.read_csv(
        csv_path,
        chunksize=CHUNK_SIZE,
        low_memory=False,
        encoding="utf-8",
    ):

        chunk_number += 1

        row_count = len(chunk)

        copy_dataframe_chunk(
            connection,
            chunk,
            table_name,
        )

        connection.commit()

        total_rows += row_count

        print(
            f"Chunk {chunk_number:,} loaded "
            f"({row_count:,} rows) "
            f"| Total: {total_rows:,}"
        )

    print()
    print(
        f"Successfully loaded {total_rows:,} rows "
        f"into {table_name}."
    )

    return total_rows


# ============================================================
# VERIFY TABLE
# ============================================================

def verify_table(
    connection,
    table_name: str,
    expected_rows: int,
) -> None:
    """
    Verify that the PostgreSQL row count matches the
    number of rows loaded from the CSV.
    """

    banner(f"VERIFYING TABLE: {table_name}")

    with connection.cursor() as cursor:

        cursor.execute(
            sql.SQL(
                "SELECT COUNT(*) FROM {}"
            ).format(
                sql.Identifier(DB_SCHEMA, table_name)
            )
        )

        actual_rows = cursor.fetchone()[0]

    print(f"Expected rows : {expected_rows:,}")
    print(f"Database rows : {actual_rows:,}")

    if actual_rows != expected_rows:

        raise RuntimeError(
            f"Row count mismatch for table '{table_name}'. "
            f"Expected {expected_rows:,}, "
            f"but PostgreSQL contains {actual_rows:,}."
        )

    print("Verification result: PASS")


# ============================================================
# CREATE KEYS AND INDEXES
# ============================================================

def create_constraints_and_indexes(connection) -> None:
    """
    Add primary keys, foreign keys, and indexes after loading.

    These operations are performed after bulk loading because
    loading is generally faster when indexes are not maintained
    during every inserted row.
    """

    banner("CREATING KEYS, CONSTRAINTS, AND INDEXES")

    with connection.cursor() as cursor:

        # ----------------------------------------------------
        # LISTINGS PRIMARY KEY
        # ----------------------------------------------------

        print("Adding primary key: listings.id")

        cursor.execute(
            """
            ALTER TABLE public.listings
            ADD CONSTRAINT listings_pkey
            PRIMARY KEY (id);
            """
        )

        # ----------------------------------------------------
        # CALENDAR PRIMARY KEY
        # ----------------------------------------------------

        print("Adding primary key: calendar.listing_id + calendar.date")

        cursor.execute(
            """
            ALTER TABLE public.calendar
            ADD CONSTRAINT calendar_pkey
            PRIMARY KEY (listing_id, date);
            """
        )

        # ----------------------------------------------------
        # REVIEWS PRIMARY KEY
        # ----------------------------------------------------

        print("Adding primary key: reviews.id")

        cursor.execute(
            """
            ALTER TABLE public.reviews
            ADD CONSTRAINT reviews_pkey
            PRIMARY KEY (id);
            """
        )

        # ----------------------------------------------------
        # FOREIGN KEYS
        # ----------------------------------------------------

        print("Adding foreign key: calendar.listing_id -> listings.id")

        cursor.execute(
            """
            ALTER TABLE public.calendar
            ADD CONSTRAINT calendar_listing_fkey
            FOREIGN KEY (listing_id)
            REFERENCES public.listings(id);
            """
        )

        print("Adding foreign key: reviews.listing_id -> listings.id")

        cursor.execute(
            """
            ALTER TABLE public.reviews
            ADD CONSTRAINT reviews_listing_fkey
            FOREIGN KEY (listing_id)
            REFERENCES public.listings(id);
            """
        )

        # ----------------------------------------------------
        # INDEXES
        # ----------------------------------------------------

        print("Creating index: calendar.listing_id")

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_calendar_listing_id
            ON public.calendar(listing_id);
            """
        )

        print("Creating index: calendar.date")

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_calendar_date
            ON public.calendar(date);
            """
        )

        print("Creating index: reviews.listing_id")

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reviews_listing_id
            ON public.reviews(listing_id);
            """
        )

        print("Creating index: reviews.date")

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reviews_date
            ON public.reviews(date);
            """
        )

    connection.commit()

    print()
    print("Keys, constraints, and indexes created successfully.")


# ============================================================
# DATABASE SUMMARY
# ============================================================

def show_database_summary(connection) -> None:
    """
    Display a final summary of the PostgreSQL tables.
    """

    banner("POSTGRESQL DATABASE SUMMARY")

    tables = [
        "listings",
        "calendar",
        "reviews",
    ]

    with connection.cursor() as cursor:

        for table_name in tables:

            cursor.execute(
                sql.SQL(
                    "SELECT COUNT(*) FROM {}"
                ).format(
                    sql.Identifier(DB_SCHEMA, table_name)
                )
            )

            row_count = cursor.fetchone()[0]

            print(
                f"{table_name:<15} "
                f"{row_count:>15,} rows"
            )


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    """
    Main program entry point.
    """

    banner("Session2-AVR-Flow - POSTGRESQL DATA LOADER")

    print("Project structure expected:")
    print()

    print("Session2-AVR-Flow/")
    print("│")
    print("├── load_postgres.py")
    print("│")
    print("└── Datasets/")
    print("    ├── listings.csv")
    print("    ├── calendar.csv")
    print("    └── reviews.csv")

    # --------------------------------------------------------
    # STEP 1
    # --------------------------------------------------------

    check_files()

    # --------------------------------------------------------
    # STEP 2
    # --------------------------------------------------------

    test_database_connection()

    connection = None

    try:

        # ----------------------------------------------------
        # CONNECT
        # ----------------------------------------------------

        banner("CONNECTING TO POSTGRESQL")

        connection = get_connection()

        print(
            f"Connected to database: {DB_NAME}"
        )

        # ----------------------------------------------------
        # CREATE TABLES
        # ----------------------------------------------------

        create_table(
            connection,
            "listings",
            LISTINGS_CSV,
        )

        create_table(
            connection,
            "calendar",
            CALENDAR_CSV,
        )

        create_table(
            connection,
            "reviews",
            REVIEWS_CSV,
        )

        # ----------------------------------------------------
        # LOAD DATA
        # ----------------------------------------------------

        listings_rows = load_csv_to_postgres(
            connection,
            LISTINGS_CSV,
            "listings",
        )

        calendar_rows = load_csv_to_postgres(
            connection,
            CALENDAR_CSV,
            "calendar",
        )

        reviews_rows = load_csv_to_postgres(
            connection,
            REVIEWS_CSV,
            "reviews",
        )

        # ----------------------------------------------------
        # VERIFY
        # ----------------------------------------------------

        verify_table(
            connection,
            "listings",
            listings_rows,
        )

        verify_table(
            connection,
            "calendar",
            calendar_rows,
        )

        verify_table(
            connection,
            "reviews",
            reviews_rows,
        )

        # ----------------------------------------------------
        # CONSTRAINTS AND INDEXES
        # ----------------------------------------------------

        create_constraints_and_indexes(
            connection
        )

        # ----------------------------------------------------
        # FINAL SUMMARY
        # ----------------------------------------------------

        show_database_summary(
            connection
        )

        banner("POSTGRESQL LOADING COMPLETED SUCCESSFULLY")

        print("Database:")
        print(DB_NAME)

        print()
        print("Tables loaded:")

        print("  [OK] listings")
        print("  [OK] calendar")
        print("  [OK] reviews")

        print()
        print("Your Session2-AVR-Flow datasets are now stored in PostgreSQL.")

        return 0

    except Exception as error:

        if connection is not None:
            connection.rollback()

        banner("POSTGRESQL LOADING FAILED")

        print(f"Error type: {type(error).__name__}")

        print()
        print("Error message:")
        print(error)

        return 1

    finally:

        if connection is not None:

            connection.close()

            print()
            print("PostgreSQL connection closed.")


# ============================================================
# PROGRAM ENTRY
# ============================================================

if __name__ == "__main__":

    sys.exit(main())