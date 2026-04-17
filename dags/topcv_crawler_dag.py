from datetime import datetime, timedelta
import csv
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable for DAG: {name}")
    return value


PROJECT_DIR = _required_env("PROJECT_DIR")
PYTHON_BIN = _required_env("PYTHON_BIN")
SCRAPER_START_PAGE = _required_env("SCRAPER_START_PAGE")
SCRAPER_MAX_PAGES = _required_env("SCRAPER_MAX_PAGES")
SCRAPER_MAX_WORKERS = _required_env("SCRAPER_MAX_WORKERS")
SCRAPER_PAGE_DELAY_MIN = _required_env("SCRAPER_PAGE_DELAY_MIN")
SCRAPER_PAGE_DELAY_MAX = _required_env("SCRAPER_PAGE_DELAY_MAX")

def notify_failure(context):
    log.error(f"""
    [ALERT] DAG FAILED
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution date: {context['execution_date']}
    """)

def validate_data(**kwargs):
    """
    Validate the CSV created by the crawler for the current Airflow run.

    Checks:
    - expected file exists for the Airflow run date
    - file is not empty
    - required columns are present
    - job_url values are present and not duplicated inside the file
    """

    project_dir = PROJECT_DIR
    execution_end = kwargs.get("data_interval_end") or kwargs.get("logical_date") or kwargs.get("execution_date")
    if execution_end is None:
        raise AirflowFailException("Unable to determine Airflow execution date for validation")

    execution_date = execution_end.strftime("%Y%m%d")
    file_name = f"topcv_jobs_{execution_date}.csv"
    output_file = os.path.join(project_dir, "data", "raw", file_name)

    required_columns = {
        "title",
        "job_url",
        "company_url",
        "salary",
        "location",
        "experience",
        "deadline",
        "tags",
        "desc_mota",
        "desc_yeucau",
        "desc_quyenloi",
        "working_addresses",
        "working_times",
        "company_name_full",
        "company_website",
        "company_size",
        "company_followers",
        "company_industry",
        "company_address",
        "company_description",
    }

    log.info(f"[INFO] Checking file: {output_file}")

    if not os.path.exists(output_file):
        raise AirflowFailException(f"No output file found from crawler: {output_file}")
    if os.path.getsize(output_file) == 0:
        raise AirflowFailException(f"Crawler output file is empty: {output_file}")

    with open(output_file, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise AirflowFailException(f"CSV header is missing: {output_file}")

        missing_columns = required_columns - set(reader.fieldnames)
        if missing_columns:
            raise AirflowFailException(
                f"CSV is missing required columns: {sorted(missing_columns)}"
            )

        row_count = 0
        job_urls = set()
        duplicate_job_urls = 0

        for row in reader:
            row_count += 1
            job_url = (row.get("job_url") or "").strip()
            if not job_url:
                raise AirflowFailException(f"Found row {row_count} without job_url: {output_file}")
            if job_url in job_urls:
                duplicate_job_urls += 1
            job_urls.add(job_url)

    if row_count == 0:
        raise AirflowFailException(f"Crawler output file contains no data rows: {output_file}")

    if duplicate_job_urls > 0:
        raise AirflowFailException(
            f"Validation failed: found {duplicate_job_urls} duplicate job_url values in {file_name}"
        )

    log.info(
        "Data validation passed for file: %s (%s rows, %s columns)",
        file_name,
        row_count,
        len(reader.fieldnames or []),
    )

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "on_failure_callback": notify_failure,
}

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

    run_crawler = BashOperator(
        task_id="run_crawler",
        bash_command=f"""
        set -e
        echo "[INFO] Starting crawler..."
        cd {PROJECT_DIR}/src/extract/
        {PYTHON_BIN} scraper.py
        echo "[INFO] Crawler completed"
        """,
        env={
            "EXECUTION_DATE": "{{ data_interval_end.strftime('%Y%m%d') }}",
            "PROJECT_DIR": PROJECT_DIR,
            "SCRAPER_START_PAGE": SCRAPER_START_PAGE,
            "SCRAPER_MAX_PAGES": SCRAPER_MAX_PAGES,
            "SCRAPER_MAX_WORKERS": SCRAPER_MAX_WORKERS,
            "SCRAPER_PAGE_DELAY_MIN": SCRAPER_PAGE_DELAY_MIN,
            "SCRAPER_PAGE_DELAY_MAX": SCRAPER_PAGE_DELAY_MAX,
        },
        append_env=True,
        retries=0,
        execution_timeout=timedelta(hours=2),
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"""
        set -e
        echo "[INFO] Starting load data to Postgres..."
        cd {PROJECT_DIR}/src/load/
        {PYTHON_BIN} load_data_to_postgres.py
        echo "[INFO] Load completed"
        """,
        env={
            "EXECUTION_DATE": "{{ data_interval_end.strftime('%Y%m%d') }}",
            "PROJECT_DIR": PROJECT_DIR,
        },
        append_env=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="topcv_dbt_dag",
        trigger_run_id="topcv_dbt_{{ data_interval_end.strftime('%Y%m%d') }}",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        conf={
            "run_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
            "source_dag_run_id": "{{ run_id }}",
        },
        retries=0,
    )

    run_crawler >> validate >> load_to_postgres >> trigger_dbt