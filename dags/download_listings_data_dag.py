"""
Upload partitioned fraud detection data to MinIO storage.

This DAG uploads processed fraud detection data to MinIO while
maintaining the directory structure for downstream processing.
"""

import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../.env"))

listings_dataset = Dataset("file:///opt/airflow/data/processed/listings/")

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    description="Download listings data from Airbnb",
    catchup=False,
    is_paused_upon_creation=False,
    schedule=[
    ],
    max_active_runs=1,
)
def download_listing_data_dag():
    """
    Define the DAG for downloading listings data from Airbnb.

    Creates a DAG that downloads listings data from Airbnb and stores it
    in the appropriate location for further processing.
    """

    download_listings_data_task = BashOperator(
        task_id="download_listings_data_task",
        bash_command=f"cd /opt/airflow && python scripts/download_listings_data.py",
        outlets=[listings_dataset],
    )

    download_listings_data_task


dag = download_listing_data_dag()
