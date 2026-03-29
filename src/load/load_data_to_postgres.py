'''
1. Connect to database
2. Find csv
3. Read data
4. Add metadata
5. Load to database
'''
import pandas as pd
from sqlalchemy import create_engine
import os
import sys
import glob
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_data_to_postgres():
    # format connection: postgresql://username:password@host:port/database
    db_url = 'postgresql://postgres:postgres@postgres:5432/topcv_dw'
    engine = create_engine(db_url)

    project_dir = os.environ.get("PROJECT_DIR", "/opt/project")
    raw_data_dir = f'{project_dir}/data/raw/'
    
    try:
        list_of_files = glob.glob(f'{raw_data_dir}*.csv')
        if not list_of_files:
            raise ValueError(f"Folder is empty or invalid path: {raw_data_dir}")
            
        latest_file = max(list_of_files, key=os.path.getctime)
        logger.info(f'Found latest file: {latest_file}')
    except ValueError as ve:
        logger.error(f"Lỗi tìm file: {ve}")
        sys.exit(1)  # error info Airflow
    
    df = pd.read_csv(latest_file)
    df["crawled_at"] = pd.Timestamp.now()
    logger.info(f"Loading {len(df)} rows of data into the database")
    
    try:
        df.to_sql(
            'topcv_jobs',
            engine,
            schema='raw',
            if_exists='append',
            index=False
        )
        logger.info("[+] Data loaded successfully!")
    except Exception as e:
        logger.error(f"Error when pushing data into the database: {e}")
        sys.exit(1)  #  error info airflow
if __name__ == "__main__":
    load_data_to_postgres()