from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable for DAG: {name}")
    return value

def notify_failure(context):
    log.error(f"""
    [DBT PIPELINE FAILED]
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution date: {context['execution_date']}
    """)

DBT_PROJECT_DIR = _required_env("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = _required_env("DBT_PROFILES_DIR")
PROJECT_DIR = _required_env("PROJECT_DIR")
PYTHON_BIN = _required_env("PYTHON_BIN")

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="topcv_dbt_dag",
    description="Production dbt DAG with layered validation",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 3, 20),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "production"],
) as dag:
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"""
        set -e
        echo "[INFO] Checking source freshness..."
        cd {DBT_PROJECT_DIR}
        dbt source freshness \
          --select source:topcv_raw.topcv_jobs \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target-path /tmp/dbt_target
        """,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"""
        set -e
        echo "[INFO] Building and testing staging & intermediate layers..."
        cd {DBT_PROJECT_DIR}
        dbt build \
                    --select path:models/staging path:models/intermediate \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target-path /tmp/dbt_target \
          --fail-fast
        """,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_snapshots = BashOperator(
        task_id="dbt_snapshots",
        bash_command=f"""
        set -e
        echo "[INFO] Executing snapshots..."
        cd {DBT_PROJECT_DIR}
        dbt snapshot \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target-path /tmp/dbt_target
        """,
        execution_timeout=timedelta(minutes=15),
    )

    dbt_mart = BashOperator(
        task_id="dbt_mart",
        bash_command=f"""
        set -e
        echo "[INFO] Building and testing mart layer..."
        cd {DBT_PROJECT_DIR}
        dbt build \
          --select path:models/marts \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target-path /tmp/dbt_target \
          --fail-fast
        """,
        execution_timeout=timedelta(minutes=30),
    )

    setup_metabase = BashOperator(
        task_id="setup_metabase",
        bash_command=f"""
        set -e
        echo "[INFO] Setting up Metabase metadata..."
        cd {PROJECT_DIR}
        export METABASE_URL=http://metabase:3000
        {PYTHON_BIN} src/metabase/setup_metabase.py
        """,
        execution_timeout=timedelta(minutes=15),
    )

    dbt_source_freshness >> dbt_staging >> dbt_snapshots >> dbt_mart >> setup_metabase