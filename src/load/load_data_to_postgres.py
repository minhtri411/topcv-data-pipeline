"""
Production-grade PostgreSQL loader with optimizations:
1. COPY instead of pandas to_sql (10-100x faster)
2. Transaction handling (atomicity, rollback on error)
3. Duplicate prevention (upsert logic)
4. Environment-based configuration
5. Proper file selection (by date, not ctime)
6. Bulk insert with proper schema
"""

import psycopg2
from psycopg2 import sql, Error as PGError
import pandas as pd
import os
import sys
import glob
import logging
from datetime import datetime
from io import StringIO
from typing import List

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_db_config():
    """
    Lấy cấu hình database từ environment variables
    Fallback to defaults chỉ cho development
    
    Environment variables:
    - DB_HOST: PostgreSQL host (default: postgres)
    - DB_PORT: PostgreSQL port (default: 5432)
    - DB_USER: Database user (default: postgres)
    - DB_PASSWORD: Database password (default: postgres)
    - DB_NAME: Database name (default: topcv_dw)
    - DB_SCHEMA: Target schema (default: raw)
    
    Production: Set all in environment
    """
    return {
        "host": _get_required_env("DB_HOST"),
        "port": int(_get_required_env("DB_PORT")),
        "user": _get_required_env("DB_USER"),
        "password": _get_required_env("DB_PASSWORD"),
        "database": _get_required_env("DB_NAME"),
        "schema": _get_required_env("DB_SCHEMA"),
    }


def find_csv_file(project_dir: str, execution_date: str = None) -> str:
    """
    Tìm CSV file một cách thông minh:
    
    Strategy:
    1. Nếu execution_date provided (từ Airflow): tìm file match ngày đó
    2. Nếu không: tìm file matching hôm nay
    3. Fallback: tìm latest file (nhưng log warning)
    
    Tại sao tốt hơn os.path.getctime():
    - ctime có thể sai trên Windows (copy time, not create time)
    - Execution date từ Airflow là ground truth
    - Match by filename pattern an toàn hơn
    
    Args:
        project_dir: Project root directory
        execution_date: Airflow execution date (format YYYYMMDD), or None
    
    Returns:
        Full path to CSV file
    """
    raw_data_dir = f'{project_dir}/data/raw/'
    
    if not os.path.exists(raw_data_dir):
        raise ValueError(f"Data directory does not exist: {raw_data_dir}")
    
    if execution_date:
        expected_filename = f"topcv_jobs_{execution_date}.csv"
        expected_path = os.path.join(raw_data_dir, expected_filename)
        
        if os.path.exists(expected_path):
            logger.info(f"[Strategy 1] Found file by Airflow execution_date: {expected_path}")
            return expected_path
        else:
            raise ValueError(
                f"Execution date provided but matching file not found: {expected_path}. "
                "Refusing fallback to avoid loading stale data as fresh."
            )
    
    today = datetime.today().strftime("%Y%m%d")
    today_path = os.path.join(raw_data_dir, f"topcv_jobs_{today}.csv")
    
    if os.path.exists(today_path):
        logger.info(f"[Strategy 2] Found file for today: {today_path}")
        return today_path
    
    logger.warning("[Strategy 3] Falling back to latest file by date pattern")
    csv_files = sorted(glob.glob(f'{raw_data_dir}topcv_jobs_*.csv'), reverse=True)
    
    if not csv_files:
        raise ValueError(f"No CSV files found in: {raw_data_dir}")
    
    latest_file = csv_files[0]
    logger.warning(f"[Fallback] Using latest file: {latest_file}")
    return latest_file


def load_data_with_copy(df: pd.DataFrame, conn, schema: str, table: str, execution_date: str = None) -> int:
    """
    Load data using COPY into temp table, then upsert into target table.

    Ưu điểm:
    - Không cần SELECT DISTINCT toàn bộ job_url từ bảng đích (scale tốt hơn)
    - Idempotent qua UNIQUE(job_url) + ON CONFLICT
    - Vẫn dùng COPY để giữ hiệu năng cao
    """
    target_columns: List[str] = [
        "job_url",
        "title",
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
        "crawled_at",
    ]

    payload = df.copy()
    payload["job_url"] = payload.get("job_url", pd.Series(dtype="string")).astype("string").str.strip()
    payload = payload[payload["job_url"].notna() & (payload["job_url"] != "")].copy()

    if payload.empty:
        logger.warning("No valid rows (job_url is null/empty) to load")
        return 0

    if "crawled_at" in payload.columns:
        payload["crawled_at"] = pd.to_datetime(payload["crawled_at"], errors="coerce")
    else:
        payload["crawled_at"] = pd.NaT

    invalid_crawled_at = payload["crawled_at"].isna().sum()
    if invalid_crawled_at > 0:
        raise ValueError(
            f"Found {invalid_crawled_at} rows with invalid/missing crawled_at. "
            "Rejecting load to prevent stale data being marked as fresh."
        )

    for column in target_columns:
        if column not in payload.columns:
            payload[column] = None
    payload = payload[target_columns]

    original_count = len(payload)
    payload.drop_duplicates(subset=["job_url"], keep="last", inplace=True)
    intra_file_duplicates = original_count - len(payload)
    if intra_file_duplicates > 0:
        logger.warning(f"Skipping {intra_file_duplicates} duplicate job_url rows inside CSV")

    csv_buffer = StringIO()
    payload.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    temp_table = "tmp_topcv_jobs_load"
    inserted_count = 0
    cursor = None

    try:
        cursor = conn.cursor()

        cursor.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {}.{} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                sql.Identifier(temp_table),
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )

        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '')").format(
            sql.Identifier(temp_table),
            sql.SQL(", ").join(map(sql.Identifier, target_columns)),
        )
        cursor.copy_expert(copy_sql, csv_buffer)

        update_columns: List[str] = [
            "title",
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
        ]

        set_clause = sql.SQL(", ").join([
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
            for col in update_columns
        ] + [
            sql.SQL("crawled_at = GREATEST({}.crawled_at, EXCLUDED.crawled_at)").format(sql.Identifier(table)),
            sql.SQL("created_at = {}.created_at").format(sql.Identifier(table)),
        ])

        changed_predicates = sql.SQL(" OR ").join([
            sql.SQL("{}.{} IS DISTINCT FROM EXCLUDED.{}").format(
                sql.Identifier(table), sql.Identifier(col), sql.Identifier(col)
            )
            for col in update_columns
        ] + [
            sql.SQL("EXCLUDED.crawled_at > {}.crawled_at").format(sql.Identifier(table))
        ])

        insert_sql = sql.SQL(
            """
            WITH upserted AS (
                INSERT INTO {}.{} AS {} ({})
                SELECT {} FROM {}
                ON CONFLICT (job_url) DO UPDATE
                SET {}
                WHERE {}
                RETURNING 1
            )
            SELECT COUNT(*) FROM upserted
            """
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, target_columns)),
            sql.SQL(", ").join(map(sql.Identifier, target_columns)),
            sql.Identifier(temp_table),
            set_clause,
            changed_predicates,
        )
        cursor.execute(insert_sql)
        inserted_count = cursor.fetchone()[0]

        conn.commit()

        skipped_existing = len(payload) - inserted_count
        if skipped_existing > 0:
            logger.info(f"Skipped {skipped_existing} unchanged rows already up-to-date in database")
        logger.info(f"[COPY] Successfully upserted {inserted_count} rows using COPY + ON CONFLICT DO UPDATE")
        return inserted_count

    except PGError as e:
        logger.error(f"[COPY] Error during bulk upsert: {e}")
        conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()


def ensure_table_exists(conn, schema: str, table: str):
    """
    Tạo table nếu chưa tồn tại
    
    Schema match với dbt models:
    - job_url: UNIQUE (primary identifier)
    - crawled_at: timestamp (when job was scraped)
    - Other fields: job details
    """
    create_table_sql = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {}.{} (
        id BIGSERIAL PRIMARY KEY,
        job_url VARCHAR(500) UNIQUE NOT NULL,
        title VARCHAR(500),
        company_url VARCHAR(500),
        salary VARCHAR(100),
        location VARCHAR(200),
        experience VARCHAR(200),
        deadline VARCHAR(50),
        tags TEXT,
        desc_mota TEXT,
        desc_yeucau TEXT,
        desc_quyenloi TEXT,
        working_addresses TEXT,
        working_times TEXT,
        company_name_full VARCHAR(500),
        company_website VARCHAR(500),
        company_size VARCHAR(100),
        company_followers VARCHAR(100),
        company_industry VARCHAR(200),
        company_address TEXT,
        company_description TEXT,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_job_url ON {}.{} (job_url);
    CREATE INDEX IF NOT EXISTS idx_crawled_at ON {}.{} (crawled_at);
    """).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.Identifier(schema),
        sql.Identifier(table)
    )
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cursor.execute(create_table_sql)

        # Backward-compatible migration for pre-existing tables created by older versions.
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )

        # Ensure ON CONFLICT(job_url) can run on legacy tables.
        cursor.execute(
            sql.SQL(
                """
                WITH ranked AS (
                    SELECT ctid,
                           ROW_NUMBER() OVER (
                               PARTITION BY job_url
                               ORDER BY COALESCE(crawled_at, created_at) DESC NULLS LAST,
                                        ctid DESC
                           ) AS rn
                    FROM {}.{}
                    WHERE job_url IS NOT NULL
                )
                DELETE FROM {}.{} t
                USING ranked r
                WHERE t.ctid = r.ctid
                  AND r.rn > 1
                """
            ).format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )

        cursor.execute(
            sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (job_url)").format(
                sql.Identifier(f"uq_{table}_job_url"),
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )

        conn.commit()
        logger.info(f"Table {schema}.{table} ready (created or already exists)")
    except PGError as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def load_data_to_postgres():
    """
    Production-grade data loader with:
    ✓ COPY bulk insert (10-100x faster than to_sql)
    ✓ Transaction handling (atomic, rollback on error)
    ✓ Duplicate prevention (ON CONFLICT by job_url)
    ✓ Environment-based config (no hardcoded credentials)
    ✓ Smart file selection (by date, not ctime)
    ✓ Proper error handling and logging
    """
    
    try:
        db_config = get_db_config()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    try:
        project_dir = _get_required_env("PROJECT_DIR")
        execution_date = _get_required_env("EXECUTION_DATE")
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    
    logger.info("=" * 70)
    logger.info("Production PostgreSQL Loader (optimized)")
    logger.info("=" * 70)
    logger.info(f"Database config: {db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
    
    try:
        csv_file = find_csv_file(project_dir, execution_date)
        logger.info(f"CSV file: {csv_file}")
    except ValueError as e:
        logger.error(f"Failed to find CSV file: {e}")
        sys.exit(1)
    
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"Read {len(df)} rows from CSV")
        
        if df.empty:
            logger.warning("CSV file is empty, nothing to load")
            return
    
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        sys.exit(1)
    
    conn = None
    try:
        conn_string = (
            f"host={db_config['host']} "
            f"port={db_config['port']} "
            f"user={db_config['user']} "
            f"password={db_config['password']} "
            f"dbname={db_config['database']}"
        )
        
        conn = psycopg2.connect(conn_string)
        logger.info("✓ Connected to PostgreSQL")
        
        conn.autocommit = False
        
        schema = db_config['schema']
        table = 'topcv_jobs'
        
        ensure_table_exists(conn, schema, table)
        
        rows_loaded = load_data_with_copy(df, conn, schema, table, execution_date)
        
        if rows_loaded > 0:
            logger.info(f"✓ Successfully loaded {rows_loaded} new jobs into {schema}.{table}")
        else:
            logger.info("No new jobs to load")
    
    except PGError as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    
    finally:
        if conn:
            conn.close()
            logger.info("✓ Database connection closed")
        logger.info("=" * 70)


if __name__ == "__main__":
    load_data_to_postgres()