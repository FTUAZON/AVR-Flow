# AVR-Flow

## A Partitioned Parallel and Distributed Occupancy Revenue Intelligence Platform for Dynamic Pricing Models in Short-Term Rental Hospitality

**Course:** MIT 261 – Parallel and Distributed Systems  
**Session:** Session 1 – Foundations in In-Memory Cluster Compute  
**Dataset:** Seattle Airbnb Open Data  
**Industry:** Short-Term Rental Hospitality

---

## Project Overview

AVR-Flow is a Session 1 parallel and distributed computing project that analyzes occupancy and estimated booked-night revenue from the Seattle Airbnb Open Data dataset.

The project uses three related files:

- `calendar.csv` — daily availability and price events
- `listings.csv` — listing/entity attributes
- `reviews.csv` — review events

The main processing path builds a calendar-and-listing working dataset, partitions the workload by `listing_id`, performs listing-level occupancy and revenue aggregation using PySpark, benchmarks bounded partition settings, and validates the parallel result against a sequential pandas baseline.

---

## Dataset

**Dataset:** Seattle Airbnb Open Data

**Kaggle source:**

https://www.kaggle.com/datasets/airbnb/seattle

### Files Used

| File | Role | Recorded Rows | Key Information |
|---|---|---:|---|
| `calendar.csv` | Event | 1,393,570 | `listing_id`, `date`, `available`, `price` |
| `listings.csv` | Entity | 3,818 | Primary key: `id` |
| `reviews.csv` | Event | 84,849 | Primary key: `id`; foreign key: `listing_id` |

### Relationships

```text
Listing (listings.csv)
        id
        |
        | 1
        |
        +-------------------- 0..* Calendar_Day
        |                         listing_id -> listings.id
        |
        +-------------------- 0..* Review
                                  listing_id -> listings.id
```

The recorded profiling execution verified:

- three related qualifying files
- genuine one-to-many relationships
- usable date fields
- event volume above the Session 1 requirement

---

## Partitioning Strategy

### Partition Key

```text
listing_id
```

`listing_id` is used because it:

- links event records to the Listing entity
- survives the calendar-to-listings join
- creates independent listing-level aggregation units
- was measured as balanced at the logical key level

Recorded key-level results:

```text
Distinct listing_id values : 3,818
Records per key            : min 365, median 365, max 365
Key-level skew             : 1.00 : 1
```

The selected final Session 1 configuration is **4 partitions**.

Benchmark settings:

```text
2 partitions
4 partitions
8 partitions
```

---

## Computational Workload

For each `listing_id`, AVR-Flow computes:

- `observed_nights`
- `occupied_nights`
- `occupancy_rate`
- `estimated_revenue`
- `average_booked_price`

The Session 1 occupancy proxy is based on:

```text
available == "f"
```

This should be interpreted as a proxy for an unavailable/booked night rather than confirmed realized revenue.

---

## Repository Structure

```text
AVR-Flow/
│
├── Datasets/
│   ├── calendar.csv
│   ├── listings.csv
│   └── reviews.csv
│
├── results/
│   ├── file_profile.json
│   ├── calendar_listing_working.parquet
│   ├── baseline_occupancy_revenue.csv
│   ├── avr_flow_benchmark.csv
│   ├── partition_sizes.csv
│   ├── partition_strategy.json
│   ├── occupancy_revenue_intelligence.parquet
│   └── validation_report.json
│
├── docs/
│   └── avr_flow_entity_model.png
│
├── architecture/
│   └── avr_flow_architecture.png
│
├── config.py
├── profile_files.py
├── load_and_join.py
├── sequential_baseline.py
├── parallel_compute.py
├── benchmark.py
├── partition_analysis.py
├── partition_strategy.py
├── render_diagrams.py
└── README.md
```

---

## Python Scripts

### `config.py`

Stores shared configuration for:

- dataset paths
- file roles
- partition key
- event-time field
- Spark settings
- benchmark settings
- output paths

### `profile_files.py`

Profiles and validates the three files.

Checks include:

- row and column counts
- candidate primary keys
- null and distinct counts
- foreign-key integrity
- one-to-many relationships
- date validity
- dataset eligibility

Output:

```text
results/file_profile.json
```

### `load_and_join.py`

Builds the working dataset.

```text
calendar.csv
    +
listings.csv
    |
    v
calendar_listing_working.parquet
```

Join:

```text
calendar.listing_id = listings.id
```

`reviews.csv` is not joined row-by-row into the calendar working dataset. Reviews are handled separately to avoid review fan-out.

### `sequential_baseline.py`

Runs the pandas reference implementation used for:

- performance comparison
- correctness validation

### `parallel_compute.py`

Runs the PySpark parallel implementation:

```text
calendar.csv + listings.csv
        |
        v
normalize / parse fields
        |
        v
derive occupancy proxy
        |
        v
derive estimated booked-night revenue
        |
        v
broadcast listings data
        |
        v
join on listing_id
        |
        v
repartition by listing_id
        |
        v
aggregate occupancy and revenue metrics
        |
        v
validate against sequential baseline
```

### `benchmark.py`

Benchmarks:

```text
2 / 4 / 8 partitions
```

### `partition_analysis.py`

Measures:

- key-level distribution
- physical Spark partition sizes
- partition skew

### `partition_strategy.py`

Evaluates partition-key candidates and records the selected strategy.

### `render_diagrams.py`

Uses Graphviz to generate:

- entity/UML diagram
- top-to-bottom architecture diagram

Reading direction:

```text
SOURCE
   |
   v
PROCESSING
   |
   v
RESULT
```

---

## Prerequisites

Install:

- Python
- Java/JDK for PySpark
- pandas
- PySpark
- Parquet support such as `pyarrow`
- Graphviz for diagram rendering

### Create a virtual environment

From the project folder:

```powershell
python -m venv .venv
```

### Activate in Command Prompt

```cmd
.venv\Scripts\activate.bat
```

### Activate in PowerShell

```powershell
.venv\Scripts\Activate.ps1
```

If PowerShell blocks script execution, use Command Prompt activation or follow your institution's/system administrator's approved execution-policy guidance.

### Install dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install pandas pyspark pyarrow
```

Check PySpark:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

Check Java:

```powershell
java -version
```

Check Graphviz:

```powershell
dot -V
```

---

## Dataset Setup

Place the required files in:

```text
AVR-Flow/Datasets/
```

Required names:

```text
calendar.csv
listings.csv
reviews.csv
```

---

## Recommended Execution Order

Run all commands from the AVR-Flow project root.

### Step 1 — Profile the dataset

```powershell
python profile_files.py
```

### Step 2 — Build the working dataset

```powershell
python load_and_join.py
```

### Step 3 — Run the sequential baseline

```powershell
python sequential_baseline.py
```

### Step 4 — Run the parallel implementation

```powershell
python parallel_compute.py
```

### Step 5 — Benchmark partition settings

```powershell
python benchmark.py
```

### Step 6 — Analyze partition balance

```powershell
python partition_analysis.py
```

### Step 7 — Evaluate the partition strategy

```powershell
python partition_strategy.py
```

### Step 8 — Render diagrams

```powershell
python render_diagrams.py
```

---

## Recorded Session 1 Results

### Dataset Profiling

```text
calendar : 1,393,570 rows x 4 columns
listings : 3,818 rows x 92 columns
reviews  : 84,849 rows x 6 columns
```

Referential integrity:

```text
calendar.listing_id -> listings.id : PASS
reviews.listing_id  -> listings.id : PASS
```

Eligibility:

```text
condition_1_three_related_files : PASS
condition_2_one_to_many         : PASS
condition_3_timestamp           : PASS
condition_4_volume              : PASS
```

### Working Dataset Join

```text
Calendar rows before : 1,393,570
Rows after join      : 1,393,570
Rows dropped         : 0
Reviews              : aggregated separately; no row-wise fan-out
```

### Sequential Baseline

```text
Median execution time : 0.9174 seconds
Result groups         : 3,818
```

### Parallel Benchmark

| Partitions | Median Execution Time | Result Groups |
|---:|---:|---:|
| 2 | 0.7014 s | 3,818 |
| 4 | 0.6426 s | 3,818 |
| 8 | 0.6607 s | 3,818 |

**Best recorded setting:**

```text
4 partitions — 0.6426 seconds
```

### Partition Analysis

| Partitions | Minimum Records | Maximum Records | Skew |
|---:|---:|---:|---:|
| 2 | 684,375 | 709,195 | 1.04 : 1 |
| 4 | 336,530 | 366,460 | 1.09 : 1 |
| 8 | 165,710 | 187,245 | 1.13 : 1 |

### Correctness Validation

```text
Tolerance: 1e-6
Correctness: PASSED

parallel_groups      : 3818
baseline_groups      : 3818
partition_keys_match : true
```

Maximum reported differences:

```text
observed_nights      : 0.0
occupied_nights      : 0.0
occupancy_rate       : 1.1102230246251565e-16
estimated_revenue    : 0.0
average_booked_price : 0.0
```

---

## Important Current Limitation

The recorded Session 1 run reported:

```text
Occupied rows            : 459,028
Occupied rows with price : 0
```

The displayed result therefore showed:

```text
estimated_revenue    : 0.0
average_booked_price : NULL
```

The correctness check still passed because the sequential and parallel implementations matched within the configured tolerance.

Based on the recorded evidence, AVR-Flow currently demonstrates:

- multi-file dataset processing
- validated one-to-many relationships
- partitioned parallel computation
- occupancy calculation
- bounded-parallelism benchmarking
- partition skew analysis
- validation against a sequential baseline

However, the recorded results do **not yet demonstrate meaningful positive estimated revenue or an operational dynamic-pricing recommendation**. The price and availability semantics should be investigated and revalidated before using the current revenue output for substantive pricing decisions.

---

## Architecture

```text
DATA SOURCES
calendar.csv
listings.csv
reviews.csv
      |
      v
PROFILE & VALIDATE
profile_files.py
      |
      v
BUILD WORKING DATASET
load_and_join.py
      |
      v
PARTITION BY listing_id
      |
      v
PARALLEL SPARK PROCESSING
parallel_compute.py
      |
      v
OCCUPANCY + REVENUE AGGREGATION
      |
      v
BENCHMARK + PARTITION ANALYSIS
benchmark.py
partition_analysis.py
partition_strategy.py
      |
      v
OCCUPANCY & REVENUE INTELLIGENCE
      |
      v
FUTURE DYNAMIC PRICING MODEL
```

Generated diagrams:

```text
docs/avr_flow_entity_model.png
architecture/avr_flow_architecture.png
```

---

## Session 2–6 Continuity

### Session 2 — Event Streaming and Replay

Use `calendar.csv` as the daily event source with:

- event time: `date`
- partition key: `listing_id`
- replay order: ascending date

### Session 3 — Distributed Services

Potential future components:

- Listing service
- Occupancy and Revenue service
- Review aggregation service

### Session 4 — Windowed Analytics

Compute occupancy and revenue indicators across:

- daily windows
- weekly windows
- monthly windows

### Session 5 — Containerization

Containerize the Session 1 runtime and dependencies.

### Session 6 — Infrastructure as Code and CI

Automate:

- profiling
- join validation
- baseline execution
- parallel execution
- benchmarking
- skew checks
- correctness assertions

---

## Reproducibility Notes

For comparable benchmark results:

- use the same dataset files
- keep the join path unchanged
- use the same workload definition
- benchmark the same partition settings
- use the same machine/runtime environment when comparing timing results

Timing can change with:

- hardware
- CPU availability
- Java version
- Python version
- PySpark version
- Spark configuration
- background processes

---

## Academic and Ethical Notes

`reviews.csv` contains reviewer-related fields and free-text comments. Avoid unnecessarily exposing raw reviewer information in derived outputs.

Any AI assistance, external references, or code assistance should be disclosed according to course requirements.

---

## Quick Start

```powershell
cd path\to\AVR-Flow

python -m venv .venv

.venv\Scripts\activate

python -m pip install pandas pyspark pyarrow

python profile_files.py
python load_and_join.py
python sequential_baseline.py
python parallel_compute.py
python benchmark.py
python partition_analysis.py
python partition_strategy.py
python render_diagrams.py
```

---

## Project Status

**Session 1 status:** Execution evidence available.

The current project provides a validated foundation for partitioned parallel processing and occupancy analytics. Revenue and dynamic-pricing interpretation should be strengthened after the recorded price/availability limitation is resolved and the resulting calculations are revalidated.
