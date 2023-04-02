from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from utils import get_savepath, load_config
import os
import pytz
from datetime import datetime, timedelta


@task(retries=1)
def extract_from_gcs(bucket_name, file_path):
    local_folder = os.getenv('DATA_PATH')
    gcs_block = GcsBucket.load(bucket_name)
    gcs_block.get_directory(from_path=file_path, local_path=local_folder)
    return Path(os.path.join(local_folder, file_path))


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # Cast to string
    df['type'] = df['type'].astype(str)
    df['actor'] = df['actor'].astype(str)
    df['repo'] = df['repo'].astype(str)
    df['payload'] = df['payload'].astype(str)
    df['public'] = df['public'].astype(str)
    df['org'] = df['org'].astype(str)
    df['public'] = df['public'].astype(bool)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("majestic-poetry")

    df.to_gbq(
        destination_table="gh_archive.raw_log",
        project_id="majestic-poetry-375216",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow
def etl_gcs_to_bigquery(year=None, month=None, day=None, hours=None, env_file='.env'):
    if year is None:
        current_time = datetime.now(pytz.utc) - timedelta(hours=1)
        year = current_time.year
        month = current_time.month
        day = current_time.day
        hours = current_time.hour
    if env_file is not None:
        load_config(env_file)
    _, __, gcs_path = get_savepath(year, month, day, hours)
    data = extract_from_gcs(os.getenv('GCS_BUCKET'), gcs_path)
    df = transform(data)
    write_bq(df)


if __name__ == '__main__':
    etl_gcs_to_bigquery(2023, 3, 26, 1)