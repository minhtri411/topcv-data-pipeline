from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

# ALERT
def notify_failure(context):
    log.error(f"""
    [ALERT] DAG FAILED
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution date: {context['execution_date']}
    """)

# DATA VALIDATION
def validate_data(**kwargs):
    """
    Validation logic (Kiểm tra file CSV có đuôi ngày tháng từ crawler)
    """
    import os

    project_dir = Variable.get("PROJECT_DIR", default_var="/opt/project")
    current_date = datetime.now().strftime("%Y%m%d")
    
    file_name = f"topcv_jobs_{current_date}.csv"
    output_file = f"{project_dir}/data/raw/{file_name}"

    log.info(f"[INFO] Checking file: {output_file}")

    if not os.path.exists(output_file):
        raise AirflowFailException(f"No output file found from crawler: {output_file}")
    if os.path.getsize(output_file) == 0:
        raise AirflowFailException(f"Crawler output file is empty: {output_file}")

    log.info(f"Data validation passed for file: {file_name}")

# DEFAULT ARGS
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "on_failure_callback": notify_failure,
}

# DAG
with DAG(
    dag_id="topcv_crawler_dag",
    description="Production DAG: Crawl → Validate → Load DB → Trigger dbt",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 3, 20),
    catchup=False,
    max_active_runs=1,
    tags=["production", "topcv", "crawler"],
) as dag:

    # TASK 1: RUN CRAWLER (EXTRACT)
    run_crawler = BashOperator(
        task_id="run_crawler",
        bash_command="""
        set -e
        echo "[INFO] Starting crawler..."
        cd {{ var.value.get('PROJECT_DIR', '/opt/project') }}/src/extract/
        {{ var.value.get('PYTHON_BIN', 'python3') }} scraper.py
        echo "[INFO] Crawler completed"
        """,
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=20),
    )

    # TASK 2: VALIDATE DATA
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )

    # TASK 3: LOAD TO POSTGRES 
    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="""
        set -e
        echo "[INFO] Starting load data to Postgres..."
        cd {{ var.value.get('PROJECT_DIR', '/opt/project') }}/src/load/
        {{ var.value.get('PYTHON_BIN', 'python3') }} load_data_to_postgres.py
        echo "[INFO] Load completed"
        """,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    # TASK 4: TRIGGER DBT DAG (TRANSFORM)
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="topcv_dbt_dag",
        wait_for_completion=False,   
        conf={
            "run_date": "{{ ds }}",
        },
        retries=0,
    )

    run_crawler >> validate >> load_to_postgres >> trigger_dbt