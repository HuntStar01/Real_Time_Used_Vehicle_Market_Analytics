# =============================================================================
# Phase 9 — Apache Airflow Orchestration
# Vehicle Market Analytics Pipeline DAG
# Pipeline: Bronze → Silver → Feature Engineering → Hive → Kafka → Streaming → HBase
# =============================================================================
#
# Requirements:
#   Python 3.11 (not 3.13 — ecosystem compatibility)
#
# Installation:
#   python3.11 -m pip install apache-airflow==3.2.1 papermill \
#     --constraint "https://raw.githubusercontent.com/apache/airflow/\
# constraints-3.2.1/constraints-3.11.txt"
#
#   python3.11 -m pip install apache-airflow-providers-standard
#
# Initialization:
#   export AIRFLOW_HOME=~/airflow
#   airflow db init
#   airflow users create \
#     --username admin --password admin \
#     --firstname Admin --lastname User \
#     --role Admin --email admin@example.com
#
# Place this file at: $AIRFLOW_HOME/dags/vehicle_market_pipeline.py
# =============================================================================

import os
import subprocess
import logging
from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# =============================================================================
# CONFIG — All paths and constants defined here. Never hardcode inside functions.
# =============================================================================

PROJECT_BASE  = os.path.expanduser("/Users/huntstar/Projects/Vehical_Project")
NOTEBOOKS_DIR = f"{PROJECT_BASE}/Project_code"
LOGS_DIR      = f"{PROJECT_BASE}/airflow_logs"

# --- HDFS Paths ---
BRONZE_PATH   = "/user/vehicle_market/bronze"
SILVER_PATH   = "/user/vehicle_market/silver/vehicles_clean"
FEATURED_PATH = "/user/vehicle_market/featured/vehicles_featured"
EXPORT_PATH   = os.path.expanduser("/Users/huntstar/Projects/Vehical_Project/tableau_data/vehicles_featured.csv")

# --- Kafka ---
KAFKA_TOPIC   = "vehicle_listings"
KAFKA_BROKER  = "localhost:9092"

# --- HBase ---
HBASE_TABLE   = "vehicle_stream"
HBASE_HOST    = "localhost"

# --- Hive ---
HIVE_DATABASE = "vehicle_market"
HIVE_TABLE    = "vehicle_featured"

# --- Sequential notebook naming ---
NOTEBOOKS = {
    "etl"       : f"{NOTEBOOKS_DIR}/01_Bronze_to_Silver_ETL.ipynb",
    "features"  : f"{NOTEBOOKS_DIR}/02_Feature_Engineering.ipynb",
    "hive"      : f"{NOTEBOOKS_DIR}/03_Hive_Data_Warehouse.ipynb",
    "kafka"     : f"{NOTEBOOKS_DIR}/04_Kafka_Producer.ipynb",
    "streaming" : f"{NOTEBOOKS_DIR}/05_Spark_Structured_Streaming.ipynb",
    "hbase"     : f"{NOTEBOOKS_DIR}/06_HBase_Storage.ipynb",
}

# Service check commands
# HBase uses HappyBase (the same interface used in the HBase phase)
# rather than the HBase shell — faster and validates the actual write path
SERVICE_CHECKS = {
    "HDFS": (
        "hdfs dfs -ls / > /dev/null 2>&1"
    ),
    "YARN": (
        "yarn node -list > /dev/null 2>&1"
    ),
    "Hive Metastore": (
        "hive -e 'SHOW DATABASES;' > /dev/null 2>&1"
    ),
    "ZooKeeper": (
        "echo ruok | nc localhost 2181 | grep -q imok"
    ),
    "Kafka": (
        f"kafka-topics.sh --bootstrap-server {KAFKA_BROKER} "
        "--list > /dev/null 2>&1"
    ),
}

PAPERMILL_CMD = "papermill {input} {output} --log-output"

# =============================================================================
# DEFAULT ARGS
# =============================================================================

default_args = {
    "owner"            : "vehicle_market",
    "depends_on_past"  : False,
    "start_date"       : datetime(2026, 7, 22),
    "email_on_failure" : False,
    "email_on_retry"   : False,
    "retries"          : 1,
    "retry_delay"      : timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def check_services(**context):
    """
    Verify all required services are running before the pipeline starts.
    HBase is validated via HappyBase connection rather than the HBase shell
    — this is faster and validates the actual interface used in the HBase phase.
    Raises immediately if any service is unavailable.
    """
    log.info("=" * 55)
    log.info("  Service Health Check")
    log.info("=" * 55)

    failed_services = []

    for service_name, check_cmd in SERVICE_CHECKS.items():
        try:
            result = subprocess.run(
                check_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                log.info(f"  ✓ {service_name:<20} — running")
            else:
                log.error(f"  ✗ {service_name:<20} — NOT RUNNING")
                log.error(f"    stderr: {result.stderr.strip()}")
                failed_services.append(service_name)
        except subprocess.TimeoutExpired:
            log.error(f"  ✗ {service_name:<20} — TIMEOUT")
            failed_services.append(service_name)
        except Exception as e:
            log.error(f"  ✗ {service_name:<20} — ERROR: {e}")
            failed_services.append(service_name)

    # Check HBase via HappyBase — validates the actual Thrift write path
    try:
        import happybase
        conn = happybase.Connection(HBASE_HOST, timeout=10000)
        conn.open()
        conn.tables()   # lightweight call to confirm connection is live
        conn.close()
        log.info(f"  ✓ {'HBase (HappyBase)':<20} — running")
    except ImportError:
        log.error(
            "  ✗ HBase (HappyBase)  — happybase not installed.\n"
            "    Install with: pip install happybase"
        )
        failed_services.append("HBase")
    except Exception as e:
        log.error(
            f"  ✗ {'HBase (HappyBase)':<20} — NOT RUNNING\n"
            f"    Details: {e}\n"
            f"    Start HBase Thrift: $HBASE_HOME/bin/hbase thrift start &"
        )
        failed_services.append("HBase")

    if failed_services:
        raise RuntimeError(
            f"The following services are not running: {failed_services}.\n"
            f"Start all required services before triggering the pipeline.\n"
            f"  HDFS/YARN  : $HADOOP_HOME/sbin/start-all.sh\n"
            f"  ZooKeeper  : $KAFKA_HOME/bin/zookeeper-server-start.sh "
            f"$KAFKA_HOME/config/zookeeper.properties &\n"
            f"  Kafka      : $KAFKA_HOME/bin/kafka-server-start.sh "
            f"$KAFKA_HOME/config/server.properties &\n"
            f"  HBase      : $HBASE_HOME/bin/start-hbase.sh\n"
            f"  HBase Thrift: $HBASE_HOME/bin/hbase thrift start &\n"
            f"  Hive       : hive --service metastore &"
        )

    log.info("=" * 55)
    log.info("  All services running. Pipeline cleared to start.")
    log.info("=" * 55)


def run_notebook(phase_name, notebook_key, **context):
    """
    Execute a Jupyter notebook using papermill.
    Logs: Started → Input → Execution → Output → Duration → Completed.
    Saves executed output notebook to LOGS_DIR with timestamp.
    Pushes duration and output path to XCom for the report task.
    Raises immediately on failure — pipeline stops.
    """
    input_nb  = NOTEBOOKS[notebook_key]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_nb = f"{LOGS_DIR}/{phase_name}_{timestamp}_output.ipynb"

    os.makedirs(LOGS_DIR, exist_ok=True)

    log.info("=" * 55)
    log.info(f"  Started   : {phase_name}")
    log.info(f"  Input     : {input_nb}")

    if not os.path.exists(input_nb):
        raise FileNotFoundError(
            f"Notebook not found: {input_nb}\n"
            f"Ensure all notebooks exist at: {NOTEBOOKS_DIR}"
        )

    cmd = PAPERMILL_CMD.format(input=input_nb, output=output_nb)

    log.info(f"  Executing : {cmd}")

    phase_start = datetime.now()

    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True
    )

    phase_end    = datetime.now()
    duration     = (phase_end - phase_start).seconds
    duration_fmt = f"{duration // 60}m {duration % 60}s"

    if result.returncode != 0:
        log.error(f"  ✗ {phase_name} FAILED after {duration_fmt}")
        log.error(f"  stdout: {result.stdout[-2000:]}")
        log.error(f"  stderr: {result.stderr[-2000:]}")
        raise RuntimeError(
            f"Phase '{phase_name}' failed. Pipeline stopped.\n"
            f"Check output notebook: {output_nb}"
        )

    log.info(f"  Output    : {output_nb}")
    log.info(f"  Duration  : {duration_fmt}")
    log.info(f"  ✓ Completed: {phase_name}")
    log.info("=" * 55)

    context["ti"].xcom_push(key=f"{phase_name}_duration", value=duration_fmt)
    context["ti"].xcom_push(key=f"{phase_name}_output_nb", value=output_nb)

    return output_nb


def validate_silver(**context):
    """
    Validate Silver layer output after Bronze → Silver ETL.
    Checks that vehicles_clean exists in HDFS and is non-empty.
    Raises immediately if missing — Feature Engineering must not run on bad data.
    """
    log.info("=" * 55)
    log.info("  Silver Validation")
    log.info("=" * 55)
    log.info(f"  Checking: {SILVER_PATH}")

    # Check path exists
    exists = subprocess.run(
        f"hdfs dfs -test -e {SILVER_PATH}",
        shell=True, capture_output=True
    )
    if exists.returncode != 0:
        raise RuntimeError(
            f"Silver validation FAILED — path missing: {SILVER_PATH}\n"
            "Review Bronze → Silver ETL before proceeding."
        )
    log.info(f"  ✓ Path exists      : {SILVER_PATH}")

    # Check non-empty
    count_result = subprocess.run(
        f"hdfs dfs -count {SILVER_PATH}",
        shell=True, capture_output=True, text=True
    )
    if count_result.returncode == 0:
        parts      = count_result.stdout.strip().split()
        file_count = int(parts[1]) if len(parts) >= 2 else 0
        if file_count == 0:
            raise RuntimeError(
                f"Silver validation FAILED — path is empty: {SILVER_PATH}\n"
                "Review Bronze → Silver ETL."
            )
        log.info(f"  ✓ Files found      : {file_count}")
    else:
        raise RuntimeError(f"Could not count files at {SILVER_PATH}")

    log.info("  ✓ Silver validation passed.")
    log.info("=" * 55)


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def _check_hdfs_path(path):
    """Check HDFS path exists. Raises immediately if missing."""
    result = subprocess.run(
        f"hdfs dfs -test -e {path}",
        shell=True, capture_output=True
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"HDFS path missing: {path}\n"
            "Pipeline stopped."
        )
    log.info(f"  ✓ Path exists      : {path}")


def _count_hdfs_files(path):
    """Return file count at HDFS path. Raises if empty or unreadable."""
    result = subprocess.run(
        f"hdfs dfs -count {path}",
        shell=True, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Could not count files at {path}")
    parts      = result.stdout.strip().split()
    file_count = int(parts[1]) if len(parts) >= 2 else 0
    if file_count == 0:
        raise RuntimeError(f"HDFS path is empty: {path}")
    log.info(f"  ✓ Files found      : {file_count} parts")
    return file_count


def _validate_parquet(path, ti):
    """
    Read parquet via PySpark. Validates:
      - readable
      - row count > 0
      - column count > 0
      - partition count
      - schema (printed line by line)
    Pushes featured_row_count to XCom.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("featured_validation") \
            .enableHiveSupport() \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        df = spark.read.parquet(f"hdfs://localhost:9000{path}")

        row_count = df.count()
        if row_count == 0:
            raise RuntimeError(
                "Featured dataset is readable but contains 0 rows."
            )

        col_count = len(df.columns)
        if col_count == 0:
            raise RuntimeError(
                "Featured dataset schema has 0 columns."
            )

        partition_count = df.rdd.getNumPartitions()

        log.info(f"  ✓ Parquet readable : yes")
        log.info(f"  ✓ Row count        : {row_count:,}")
        log.info(f"  ✓ Column count     : {col_count}")
        log.info(f"  ✓ Partitions       : {partition_count}")
        log.info(f"  ✓ Schema           :")
        for field in df.schema.fields:
            log.info(f"      {field.name:<30} {str(field.dataType)}")

        ti.xcom_push(key="featured_row_count", value=row_count)
        spark.stop()

    except ImportError:
        log.warning(
            "  ⚠ PySpark not available in Airflow worker — "
            "skipping row count and schema check."
        )


# =============================================================================
# FEATURED DATASET VALIDATION
# =============================================================================

def validate_featured_dataset(**context):
    """
    Quality gate before Hive registration.
    Delegates to three focused helpers.
    Pipeline stops immediately if any check fails.
    """
    log.info("=" * 55)
    log.info("  Featured Dataset Validation")
    log.info("=" * 55)

    _check_hdfs_path(FEATURED_PATH)
    _count_hdfs_files(FEATURED_PATH)
    _validate_parquet(FEATURED_PATH, context["ti"])

    log.info("  ✓ Featured dataset validation passed.")
    log.info("=" * 55)


def validate_hive_table(**context):
    """
    Validate Hive external table after Hive Data Warehouse phase.
    Checks:
      - Hive database exists
      - External table exists
      - Row count matches parquet row count
      - Schema is non-empty
      - SQL query executes successfully
    Pipeline stops if any check fails.
    """
    log.info("=" * 55)
    log.info("  Hive Validation")
    log.info("=" * 55)

    def run_hive(query):
        return subprocess.run(
            f"hive -e \"{query}\"",
            shell=True, capture_output=True, text=True, timeout=120
        )

    # 1. Database exists
    db_result = run_hive(f"SHOW DATABASES LIKE '{HIVE_DATABASE}';")
    if HIVE_DATABASE not in db_result.stdout:
        raise RuntimeError(
            f"Hive validation FAILED — database not found: {HIVE_DATABASE}"
        )
    log.info(f"  ✓ Database exists  : {HIVE_DATABASE}")

    # 2. Table exists
    tbl_result = run_hive(f"USE {HIVE_DATABASE}; SHOW TABLES LIKE '{HIVE_TABLE}';")
    if HIVE_TABLE not in tbl_result.stdout:
        raise RuntimeError(
            f"Hive validation FAILED — table not found: {HIVE_TABLE}\n"
            "Review Hive Data Warehouse notebook."
        )
    log.info(f"  ✓ Table exists     : {HIVE_TABLE}")

    # 3. Row count
    count_result = run_hive(
        f"USE {HIVE_DATABASE}; SELECT COUNT(*) FROM {HIVE_TABLE};"
    )
    hive_rows = None
    for line in count_result.stdout.strip().splitlines():
        if line.strip().isdigit():
            hive_rows = int(line.strip())
            break

    if hive_rows is None or hive_rows == 0:
        raise RuntimeError(
            f"Hive validation FAILED — table {HIVE_TABLE} returned 0 rows or count failed."
        )
    log.info(f"  ✓ Row count        : {hive_rows:,}")
    featured_rows = context["ti"].xcom_pull(
    task_ids="feature_validation", key="featured_row_count"
    )
    if featured_rows is not None and hive_rows != featured_rows:
        raise RuntimeError(
            f"Row count mismatch — Parquet: {featured_rows:,}, Hive: {hive_rows:,}"
        )

    # 4. Schema check
    desc_result = run_hive(f"USE {HIVE_DATABASE}; DESCRIBE {HIVE_TABLE};")
    if desc_result.returncode != 0 or not desc_result.stdout.strip():
        raise RuntimeError(
            f"Hive validation FAILED — could not describe table {HIVE_TABLE}."
        )
    log.info(f"  ✓ Schema           : readable")

    # 5. SQL query executes
    query_result = run_hive(
        f"USE {HIVE_DATABASE}; "
        f"SELECT manufacturer, COUNT(*) as cnt "
        f"FROM {HIVE_TABLE} GROUP BY manufacturer LIMIT 5;"
    )
    if query_result.returncode != 0:
        raise RuntimeError(
            f"Hive validation FAILED — test SQL query failed on {HIVE_TABLE}."
        )
    log.info(f"  ✓ SQL query        : executed successfully")

    context["ti"].xcom_push(key="hive_row_count", value=hive_rows)

    log.info("  ✓ Hive validation passed.")
    log.info("=" * 55)


def export_and_validate_csv(**context):
    """
    Validate the CSV already exported by the Hive notebook (coalesce(1)).
    Airflow does not re-export — it validates what the notebook produced.
    Checks: file exists, size > 0, row count > 0, header present.
    Pipeline stops immediately if any check fails.
    """
    log.info("=" * 55)
    log.info("  CSV Export Validation")
    log.info("=" * 55)
    log.info(f"  Validating: {EXPORT_PATH}")

    # 1. File exists
    if not os.path.exists(EXPORT_PATH):
        raise RuntimeError(
            f"CSV validation FAILED — file not found: {EXPORT_PATH}\n"
            "Ensure the Hive notebook exports the CSV before this task runs."
        )
    log.info(f"  ✓ File exists      : {EXPORT_PATH}")

    # 2. File size > 0
    file_size = os.path.getsize(EXPORT_PATH)
    if file_size == 0:
        raise RuntimeError(
            f"CSV validation FAILED — file is empty: {EXPORT_PATH}"
        )
    log.info(f"  ✓ File size        : {file_size / (1024 * 1024):.2f} MB")

    # 3. Row count > 1 (at least header + one data row)
    with open(EXPORT_PATH, "r") as f:
        row_count = sum(1 for _ in f)
    if row_count <= 1:
        raise RuntimeError(
            f"CSV validation FAILED — only {row_count} line(s) found "
            f"(expected header + data rows)."
        )
    data_rows = row_count - 1
    log.info(f"  ✓ Row count        : {data_rows:,} rows (+ 1 header)")

    # 4. Header present and non-empty
    with open(EXPORT_PATH, "r") as f:
        header = f.readline().strip()
    if not header:
        raise RuntimeError(
            "CSV validation FAILED — header row is missing or empty."
        )
    log.info(f"  ✓ Header           : {header[:80]}{'...' if len(header) > 80 else ''}")

    context["ti"].xcom_push(key="csv_rows_exported", value=data_rows)

    log.info("  ✓ CSV validation passed.")
    log.info("=" * 55)


def generate_pipeline_report(**context):
    """
    Collect XCom metadata from all phases and log a final
    pipeline execution summary.
    Reports: phase durations, rows processed, rows exported,
    Kafka messages, HBase rows, and total pipeline duration.
    Uses logical_date (Airflow 3) instead of execution_date (Airflow 2).
    """
    ti = context["ti"]

    phases = [
        ("phase01_etl",        "Bronze → Silver ETL"),
        ("phase02_features",   "Feature Engineering"),
        ("feature_validation", "Feature Dataset Validation"),
        ("phase03_hive",       "Hive Data Warehouse"),
        ("hive_validation",    "Hive Validation"),
        ("csv_export",         "CSV Export Validation"),
        ("phase04_kafka",      "Kafka Producer"),
        ("phase05_streaming",  "Spark Structured Streaming"),
        ("phase06_hbase",      "HBase Storage"),
    ]

    # Collect metrics from XCom
    featured_rows  = ti.xcom_pull(task_ids="feature_validation", key="featured_row_count") or "N/A"
    hive_rows      = ti.xcom_pull(task_ids="hive_validation",    key="hive_row_count")     or "N/A"
    csv_rows       = ti.xcom_pull(task_ids="csv_export",         key="csv_rows_exported")  or "N/A"
    def _read_xcom_file(path):
        try:
            with open(os.path.expanduser(path)) as f:
                return json.load(f)
        except Exception:
            return {}

    kafka_data     = _read_xcom_file("~/airflow_logs/kafka_xcom.json")
    hbase_data     = _read_xcom_file("~/airflow_logs/hbase_xcom.json")
    kafka_messages = kafka_data.get("kafka_messages_sent", "N/A")
    hbase_rows     = hbase_data.get("hbase_rows_written",  "N/A")

    log.info("\n" + "=" * 65)
    log.info("    VEHICLE MARKET PIPELINE — EXECUTION REPORT")
    log.info("=" * 65)
    log.info(f"  DAG Run      : {context['run_id']}")
    log.info(f"  Logical Date : {context['logical_date']}")
    log.info(f"  Triggered By : {context['dag_run'].run_type}")
    log.info("")
    log.info(f"  {'Phase':<35} {'Duration':>10}  {'Output Notebook'}")
    log.info(f"  {'-' * 70}")

    for task_id, label in phases:
        duration  = ti.xcom_pull(task_ids=task_id, key=f"{task_id}_duration") or "—"
        output_nb = ti.xcom_pull(task_ids=task_id, key=f"{task_id}_output_nb") or "—"
        nb_name   = os.path.basename(output_nb) if output_nb != "—" else "—"
        log.info(f"  {label:<35} {duration:>10}  {nb_name}")

    log.info("")
    log.info("  Pipeline Metrics")
    log.info(f"  {'-' * 40}")
    log.info(f"  Rows Processed (Featured)  : {featured_rows:,}" if isinstance(featured_rows, int) else f"  Rows Processed (Featured)  : {featured_rows}")
    log.info(f"  Rows in Hive Table         : {hive_rows:,}"     if isinstance(hive_rows, int)     else f"  Rows in Hive Table         : {hive_rows}")
    log.info(f"  Rows Exported (CSV)        : {csv_rows:,}"      if isinstance(csv_rows, int)      else f"  Rows Exported (CSV)        : {csv_rows}")
    log.info(f"  Kafka Messages Sent        : {kafka_messages}")
    log.info(f"  HBase Rows Written         : {hbase_rows}")
    log.info("")
    log.info("  ✓ All phases completed successfully.")
    log.info("  ✓ Featured CSV exported to: " + os.path.dirname(EXPORT_PATH))
    log.info("  ✓ Ready for Tableau dashboard development.")
    log.info("=" * 65)


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="vehicle_market_pipeline",
    description=(
        "End-to-end Vehicle Market Analytics Pipeline — "
        "Bronze → Silver → Feature Engineering → Hive → Kafka → Streaming → HBase"
    ),
    default_args=default_args,
    schedule=None,              # manual trigger only — dataset is static
                                # change to "@daily" for scheduled runs
    catchup=False,
    max_active_runs=1,          # prevent concurrent runs writing to same HDFS paths
    tags=["vehicle_market", "big_data", "cdac"],
) as dag:

    # =========================================================================
    # MARKERS
    # EmptyOperator replaces DummyOperator in Airflow 3
    # =========================================================================

    pipeline_start = EmptyOperator(task_id="pipeline_start")
    pipeline_end   = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # =========================================================================
    # SERVICE HEALTH CHECK
    # Validates HDFS, YARN, Hive Metastore, ZooKeeper, Kafka, HBase
    # before any notebook runs. Raises immediately if any service is down.
    # =========================================================================

    service_check = PythonOperator(
        task_id="service_validation",
        python_callable=check_services,
        execution_timeout=timedelta(minutes=5),
        retries=0,
    )

    # =========================================================================
    # PHASE 1 — Bronze → Silver ETL
    # Pipeline begins here immediately after service validation.
    # =========================================================================

    phase01_etl = PythonOperator(
        task_id="phase01_etl",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase01_etl", "notebook_key": "etl"},
        execution_timeout=timedelta(hours=2),
        retries=1,
    )

    # =========================================================================
    # SILVER VALIDATION GATE
    # Validates vehicles_clean only.
    # Feature Engineering produces vehicles_featured — validated separately.
    # =========================================================================

    silver_validation = PythonOperator(
        task_id="silver_validation",
        python_callable=validate_silver,
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    # =========================================================================
    # PHASE 2 — Feature Engineering
    # Produces the final analytical dataset (vehicles_featured).
    # =========================================================================

    phase02_features = PythonOperator(
        task_id="phase02_features",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase02_features", "notebook_key": "features"},
        execution_timeout=timedelta(hours=1),
        retries=1,
    )

    # =========================================================================
    # FEATURE DATASET VALIDATION GATE
    # Quality gate before Hive registration.
    # Validates: path exists, parquet readable, row count > 0,
    # schema non-empty, partition count, prints summary.
    # Pipeline stops immediately if any check fails.
    # =========================================================================

    feature_validation = PythonOperator(
        task_id="feature_validation",
        python_callable=validate_featured_dataset,
        execution_timeout=timedelta(minutes=15),
        retries=0,
    )

    # =========================================================================
    # PHASE 3 — Hive Data Warehouse
    # Registers vehicles_featured as a single external table.
    # Runs after feature_validation passes.
    # =========================================================================

    phase03_hive = PythonOperator(
        task_id="phase03_hive",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase03_hive", "notebook_key": "hive"},
        execution_timeout=timedelta(hours=1),
        retries=1,
    )

    # =========================================================================
    # HIVE VALIDATION GATE
    # Verifies: database exists, external table exists,
    # row count matches parquet, schema readable, SQL query works.
    # =========================================================================

    hive_validation = PythonOperator(
        task_id="hive_validation",
        python_callable=validate_hive_table,
        execution_timeout=timedelta(minutes=15),
        retries=0,
    )

    # =========================================================================
    # CSV EXPORT + VALIDATION
    # Exports Featured Dataset from HDFS to local CSV for Tableau.
    # Validates: file exists, size > 0, row count > 0, header present.
    # Airflow stops here — Tableau consumes this CSV as a BI layer,
    # not as an ETL task.
    # =========================================================================

    csv_export = PythonOperator(
        task_id="csv_export",
        python_callable=export_and_validate_csv,
        execution_timeout=timedelta(minutes=15),
        retries=1,
    )

    # =========================================================================
    # PHASE 4 — Kafka Producer
    # Streams Featured Dataset records into Kafka topic.
    # Reports: topic, message count, throughput, duration, failed messages.
    # =========================================================================

    phase04_kafka = PythonOperator(
        task_id="phase04_kafka",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase04_kafka", "notebook_key": "kafka"},
        execution_timeout=timedelta(minutes=30),
        retries=1,
    )

    # =========================================================================
    # PHASE 5 — Spark Structured Streaming
    # Consumes Kafka topic and processes live vehicle records.
    # Reports: Streaming Started → Batch ID → Rows → Batch Duration →
    #          Processing Rate → Termination.
    # retries=0 — streaming failures can corrupt checkpoint state.
    # =========================================================================

    phase05_streaming = PythonOperator(
        task_id="phase05_streaming",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase05_streaming", "notebook_key": "streaming"},
        execution_timeout=timedelta(minutes=15),
        retries=0,
    )

    # =========================================================================
    # PHASE 6 — HBase Storage
    # Writes streaming output to HBase.
    # Validates: write → read back → verify row → count rows → continue.
    # Never assumes writes succeeded.
    # =========================================================================

    phase06_hbase = PythonOperator(
        task_id="phase06_hbase",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase06_hbase", "notebook_key": "hbase"},
        execution_timeout=timedelta(hours=1),
        retries=1,
    )

    # =========================================================================
    # PIPELINE REPORT
    # Collects XCom from all phases.
    # Reports: phase durations, rows processed, rows exported,
    # Kafka messages, HBase rows.
    # =========================================================================

    pipeline_report = PythonOperator(
        task_id="pipeline_report",
        python_callable=generate_pipeline_report,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )



    pipeline_start     >> service_check
    service_check      >> phase01_etl
    phase01_etl        >> silver_validation
    silver_validation  >> phase02_features
    phase02_features   >> feature_validation
    feature_validation >> phase03_hive
    phase03_hive       >> hive_validation
    hive_validation    >> phase04_kafka
    phase04_kafka      >> phase05_streaming
    phase05_streaming  >> phase06_hbase
    phase06_hbase      >> csv_export
    csv_export         >> pipeline_report
    pipeline_report    >> pipeline_end
