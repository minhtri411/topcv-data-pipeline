from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

# ALERT
def notify_failure(context):
    log.error(f"""
    [DBT PIPELINE FAILED]
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution date: {context['execution_date']}
    """)

# CONFIG (safe with Jinja)
DBT_PROJECT_DIR = "{{ var.value.get('DBT_PROJECT_DIR', '/opt/project/dbt_transform') }}"
DBT_PROFILES_DIR = "{{ var.value.get('DBT_PROFILES_DIR', '/opt/project/dbt_transform') }}"

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


# DAG
with DAG(
    dag_id="topcv_dbt_dag",
    description="Production dbt DAG with layered validation",
    default_args=default_args,
    schedule_interval=None,   # trigger từ crawler DAG
    start_date=datetime(2026, 3, 20),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "production"],
) as dag:

    # 1. DBT DEPS
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        set -e
        echo "[INFO] Installing dbt dependencies..."
        cd {DBT_PROJECT_DIR}
        dbt deps --profiles-dir {DBT_PROFILES_DIR}
        """,
        execution_timeout=timedelta(minutes=5),
    )

    # 2. BUILD STAGING + INTERMEDIATE
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"""
        set -e
        echo "[INFO] Building staging & intermediate layers..."
        cd {DBT_PROJECT_DIR}
        dbt run \
          --select staging intermediate \
          --profiles-dir . \
          --target-path /tmp/dbt_target
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # 3. TEST STAGING 
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"""
        set -e
        echo "[INFO] Testing staging layer..."
        cd {DBT_PROJECT_DIR}
        dbt test \
          --select staging \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target-path /tmp/dbt_target
        """,
        execution_timeout=timedelta(minutes=10),
    )

    # 4. BUILD MART
    dbt_mart = BashOperator(
        task_id="dbt_mart",
        bash_command=f"""
        set -e
        echo "[INFO] Building mart layer..."
        cd {DBT_PROJECT_DIR}
        dbt run \
          --select marts \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target-path /tmp/dbt_target
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # 5. TEST MART
    dbt_test_mart = BashOperator(
        task_id="dbt_test_mart",
        bash_command=f"""
        set -e
        echo "[INFO] Testing mart layer..."
        cd {DBT_PROJECT_DIR}
        dbt test \
          --select marts \
          --profiles-dir {DBT_PROFILES_DIR} \
          --fail-fast \
          --target-path /tmp/dbt_target
        """,
        execution_timeout=timedelta(minutes=15),
    )

    # 6. GENERATE DOCS
    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"""
        set -e
        echo "[INFO] Generating dbt docs..."
        cd {DBT_PROJECT_DIR}
        dbt docs generate \
          --profiles-dir {DBT_PROFILES_DIR} \
          --target-path /tmp/dbt_target
        """,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_deps >> dbt_staging >> dbt_test_staging >> dbt_mart >> dbt_test_mart >> dbt_docs