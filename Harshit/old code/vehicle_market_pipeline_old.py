# =============================================================================
# Phase 9 — Apache Airflow Orchestration
# Vehicle Market Analytics Pipeline DAG
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

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# =============================================================================
# CONFIG
# =============================================================================

PROJECT_BASE  = os.path.expanduser("/Users/huntstar/Projects/Vehical_Project")
NOTEBOOKS_DIR = f"{PROJECT_BASE}/Project_code"
LOGS_DIR      = f"{PROJECT_BASE}/airflow_logs"

NOTEBOOKS = {
    "etl"       : f"{NOTEBOOKS_DIR}/01_Bronze_to_Silver_ETL.ipynb",
    "features"  : f"{NOTEBOOKS_DIR}/03_Feature_Engineering.ipynb",
    "gold"      : f"{NOTEBOOKS_DIR}/04_Gold_ETL.ipynb",
    "hive"      : f"{NOTEBOOKS_DIR}/05_Hive_Data_Warehouse.ipynb",
    "kafka"     : f"{NOTEBOOKS_DIR}/06_Kafka_Producer.ipynb",
    "streaming" : f"{NOTEBOOKS_DIR}/07_Spark_Structured_Streaming.ipynb",
    "hbase"     : f"{NOTEBOOKS_DIR}/08_HBase_Storage.ipynb",
}

# Gold HDFS paths validated before Hive table creation
GOLD_PATHS = [
    "/user/vehicle_market/gold/kpi_summary",
    "/user/vehicle_market/gold/manufacturer_summary",
    "/user/vehicle_market/gold/state_summary",
    "/user/vehicle_market/gold/monthly_summary",
    "/user/vehicle_market/gold/fuel_summary",
    "/user/vehicle_market/gold/segment_summary",
    "/user/vehicle_market/gold/condition_summary",
    "/user/vehicle_market/gold/title_summary",
    "/user/vehicle_market/gold/transmission_summary",
    "/user/vehicle_market/gold/state_manufacturer",
    "/user/vehicle_market/gold/manufacturer_condition",
]

PAPERMILL_CMD = (
    "/Users/huntstar/airflow_311/.venv/bin/papermill "
    "{input} {output} --log-output --kernel airflow_venv"
)

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

def run_notebook(phase_name, notebook_key, **context):
    """
    Execute a Jupyter notebook using papermill.
    Saves executed output notebook to LOGS_DIR with timestamp.
    Pushes duration and output path to XCom for the report task.
    """
    input_nb  = NOTEBOOKS[notebook_key]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_nb = f"{LOGS_DIR}/{phase_name}_{timestamp}_output.ipynb"

    os.makedirs(LOGS_DIR, exist_ok=True)

    if not os.path.exists(input_nb):
        raise FileNotFoundError(
            f"Notebook not found: {input_nb}\n"
            f"Ensure all notebooks exist at: {NOTEBOOKS_DIR}"
        )

    cmd = PAPERMILL_CMD.format(input=input_nb, output=output_nb)

    log.info(f"Executing: {input_nb}")
    log.info(f"Output   : {output_nb}")

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
            f"Phase '{phase_name}' failed. "
            f"Check output notebook: {output_nb}"
        )

    log.info(f"  ✓ {phase_name} completed in {duration_fmt}")

    context["ti"].xcom_push(
        key=f"{phase_name}_duration", value=duration_fmt
    )
    context["ti"].xcom_push(
        key=f"{phase_name}_output_nb", value=output_nb
    )

    return output_nb


def validate_hdfs_silver(**context):
    """
    Verify Silver layer HDFS outputs exist after ETL.
    Uses hdfs CLI — avoids PySpark import complexity in Airflow worker.
    Raises if any expected path is missing.
    """
    SILVER_PATHS = [
        "/user/vehicle_market/silver/vehicles_clean",
        "/user/vehicle_market/silver/vehicles_featured",
    ]

    log.info("Validating Silver HDFS outputs...")
    missing = []

    for path in SILVER_PATHS:
        result = subprocess.run(
            f"hdfs dfs -test -e {path}",
            shell=True, capture_output=True
        )
        if result.returncode == 0:
            log.info(f"  ✓ {path}")
        else:
            log.error(f"  ✗ MISSING: {path}")
            missing.append(path)

    if missing:
        raise RuntimeError(
            f"Silver layer outputs missing: {missing}. "
            f"Review Phase 2 and Phase 3 logs."
        )

    log.info("  Silver layer validation passed.")


def validate_gold_outputs(**context):
    """
    Verify all Gold HDFS paths exist and contain data before
    Hive external tables are created.
    Validates:
      - folder exists in HDFS
      - folder is non-empty (at least one Parquet file present)
    """
    log.info("Validating Gold HDFS outputs...")
    missing  = []
    empty    = []

    for path in GOLD_PATHS:
        # Check folder exists
        exists_result = subprocess.run(
            f"hdfs dfs -test -e {path}",
            shell=True, capture_output=True
        )
        if exists_result.returncode != 0:
            log.error(f"  ✗ MISSING     : {path}")
            missing.append(path)
            continue

        # Check folder is non-empty
        count_result = subprocess.run(
            f"hdfs dfs -count {path}",
            shell=True, capture_output=True, text=True
        )
        if count_result.returncode == 0:
            parts      = count_result.stdout.strip().split()
            file_count = int(parts[1]) if len(parts) >= 2 else 0
            if file_count == 0:
                log.error(f"  ✗ EMPTY       : {path}")
                empty.append(path)
            else:
                log.info(f"  ✓ {path} ({file_count} files)")
        else:
            log.warning(f"  ⚠ Count check failed for {path} — skipping")

    issues = missing + empty
    if issues:
        raise RuntimeError(
            f"Gold validation failed.\n"
            f"  Missing : {missing}\n"
            f"  Empty   : {empty}\n"
            f"Review Phase 4 (Gold ETL) before proceeding to Hive."
        )

    log.info(f"  All {len(GOLD_PATHS)} Gold tables validated.")


def generate_pipeline_report(**context):
    """
    Collect XCom metadata from all phases and log a final
    pipeline execution summary.
    Uses logical_date (Airflow 3) instead of execution_date (Airflow 2).
    """
    ti = context["ti"]

    phases = [
        "phase2_etl",
        "phase3_features",
        "phase4_gold",
        "phase5_hive",
        "phase6_kafka",
        "phase7_streaming",
        "phase8_hbase",
    ]

    log.info("\n" + "="*60)
    log.info("    VEHICLE MARKET PIPELINE — EXECUTION REPORT")
    log.info("="*60)
    log.info(f"  DAG Run       : {context['run_id']}")
    log.info(f"  Logical Date  : {context['logical_date']}")
    log.info(f"  Triggered By  : {context['dag_run'].run_type}")
    log.info("")
    log.info(f"  {'Phase':<30} {'Duration':>12}  {'Output Notebook'}")
    log.info(f"  {'-'*70}")

    for phase in phases:
        duration  = ti.xcom_pull(
            task_ids=phase, key=f"{phase}_duration"
        ) or "N/A"
        output_nb = ti.xcom_pull(
            task_ids=phase, key=f"{phase}_output_nb"
        ) or "N/A"
        nb_name = os.path.basename(output_nb) if output_nb != "N/A" else "N/A"
        log.info(f"  {phase:<30} {duration:>12}  {nb_name}")

    log.info("")
    log.info("  ✓ All phases completed successfully.")
    log.info("  ✓ Pipeline execution validated.")
    log.info("  ✓ Gold CSVs exported to ~/tableau_data/")
    log.info("  ✓ Ready for Tableau dashboard development.")
    log.info("="*60)


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="vehicle_market_pipeline",
    description=(
        "End-to-end Vehicle Market Analytics Pipeline — "
        "Bronze → Silver → Gold → Hive → Kafka → Streaming → HBase"
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
    # PHASE 2 — Bronze → Silver ETL
    # =========================================================================

    phase2_etl = PythonOperator(
        task_id="phase2_etl",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase2_etl", "notebook_key": "etl"},
        execution_timeout=timedelta(hours=2),
        retries=1,
    )

    # =========================================================================
    # SILVER VALIDATION GATE
    # =========================================================================

    silver_validation = PythonOperator(
        task_id="silver_validation",
        python_callable=validate_hdfs_silver,
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    # =========================================================================
    # PHASE 3 — Feature Engineering
    # =========================================================================

    phase3_features = PythonOperator(
        task_id="phase3_features",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase3_features", "notebook_key": "features"},
        execution_timeout=timedelta(hours=1),
        retries=1,
    )

    # =========================================================================
    # PHASE 4 — Gold ETL
    # =========================================================================

    phase4_gold = PythonOperator(
        task_id="phase4_gold",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase4_gold", "notebook_key": "gold"},
        execution_timeout=timedelta(hours=1),
        retries=1,
    )

    # =========================================================================
    # GOLD VALIDATION GATE
    # Verifies all 11 Gold folders exist and are non-empty
    # before Hive external tables are created
    # =========================================================================

    gold_validation = PythonOperator(
        task_id="gold_validation",
        python_callable=validate_gold_outputs,
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    # =========================================================================
    # PHASE 5 — Hive Data Warehouse
    # Runs after gold_validation passes
    # =========================================================================

    phase5_hive = PythonOperator(
        task_id="phase5_hive",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase5_hive", "notebook_key": "hive"},
        execution_timeout=timedelta(hours=1),
        retries=1,
    )

    # =========================================================================
    # EXPORT GOLD CSVs — runs in parallel with phase5_hive
    # Both depend only on gold_validation passing
    # =========================================================================

    export_gold_csv = BashOperator(
        task_id="export_gold_csv",
        bash_command="""
            mkdir -p ~/tableau_data

            for TABLE in kpi_summary manufacturer_summary state_summary \
                monthly_summary fuel_summary segment_summary \
                condition_summary title_summary transmission_summary \
                state_manufacturer manufacturer_condition; do

                echo "Exporting $TABLE..."
                hdfs dfs -getmerge \
                    /user/vehicle_market/gold/$TABLE \
                    ~/tableau_data/$TABLE.csv

                if [ $? -eq 0 ]; then
                    ROWS=$(wc -l < ~/tableau_data/$TABLE.csv)
                    echo "  ✓ $TABLE — $ROWS rows"
                else
                    echo "  ✗ $TABLE FAILED"
                    exit 1
                fi
            done

            echo "All Gold tables exported to ~/tableau_data/"
        """,
        execution_timeout=timedelta(minutes=15),
        retries=1,
    )

    # =========================================================================
    # PHASE 6 — Kafka Producer
    # =========================================================================

    phase6_kafka = PythonOperator(
        task_id="phase6_kafka",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase6_kafka", "notebook_key": "kafka"},
        execution_timeout=timedelta(minutes=30),
        retries=1,
    )

    # =========================================================================
    # PHASE 7 — Spark Structured Streaming
    # retries=0 — streaming failures can corrupt checkpoint state
    # =========================================================================

    phase7_streaming = PythonOperator(
        task_id="phase7_streaming",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase7_streaming", "notebook_key": "streaming"},
        execution_timeout=timedelta(minutes=15),
        retries=0,
    )

    # =========================================================================
    # PHASE 8 — HBase Storage
    # =========================================================================

    phase8_hbase = PythonOperator(
        task_id="phase8_hbase",
        python_callable=run_notebook,
        op_kwargs={"phase_name": "phase8_hbase", "notebook_key": "hbase"},
        execution_timeout=timedelta(hours=1),
        retries=1,
    )

    # =========================================================================
    # PIPELINE REPORT
    # =========================================================================

    pipeline_report = PythonOperator(
        task_id="pipeline_report",
        python_callable=generate_pipeline_report,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # =========================================================================
    # TASK DEPENDENCIES
    #
    # pipeline_start
    #        │
    #        ▼
    # phase2_etl
    #        │
    #        ▼
    # silver_validation          ← gate: Silver must exist
    #        │
    #        ▼
    # phase3_features
    #        │
    #        ▼
    # phase4_gold
    #        │
    #        ▼
    # gold_validation            ← gate: all 11 Gold tables must exist + non-empty
    #        │
    #   ┌────┴────┐
    #   ▼         ▼
    # phase5_hive  export_gold_csv    ← parallel: both only need Gold ready
    #   │
    #   ▼
    # phase6_kafka
    #   │
    #   ▼
    # phase7_streaming
    #   │
    #   ▼
    # phase8_hbase
    #   │
    #   ▼
    # pipeline_report
    #   │
    #   ▼
    # pipeline_end
    # =========================================================================

    pipeline_start    >> phase2_etl
    phase2_etl        >> silver_validation
    silver_validation >> phase3_features
    phase3_features   >> phase4_gold
    phase4_gold       >> gold_validation
    gold_validation   >> [phase5_hive, export_gold_csv]
    phase5_hive       >> phase6_kafka
    phase6_kafka      >> phase7_streaming
    phase7_streaming  >> phase8_hbase
    phase8_hbase      >> pipeline_report
    pipeline_report   >> pipeline_end