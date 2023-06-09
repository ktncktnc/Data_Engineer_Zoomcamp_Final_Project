from prefect import flow, task
import gzip
import json
from requests import get
import pytz
import os
from datetime import datetime, timedelta
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from utils import mkdir_if_not_exist, load_config, get_savepath, get_file_link


@task(retries=3)
def get_file(url, json_path):
    mkdir_if_not_exist(os.path.dirname(json_path))
    with open(json_path, 'wb+') as file:
        response = get(url)
        file.write(response.content)


@task(retries=1)
def preprocess_file(json_path, parquet_path):
    mkdir_if_not_exist(os.path.dirname(parquet_path))
    with gzip.open(json_path, mode="rt", encoding="utf8") as f:
        data = [json.loads(line) for line in f]

    df = pd.DataFrame.from_records(data)
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%dT%H:%M:%SZ')
    df.to_parquet(parquet_path, compression='gzip')


@task(retries=3)
def upload_to_gcs(gcs_bucket, parquet_path, gcs_path):
    gcs_block = GcsBucket.load(gcs_bucket)
    gcs_block.upload_from_path(from_path=parquet_path, to_path=gcs_path)


@task(retries=1)
def remove_files(json_path, parquet_path):
    os.remove(json_path)
    os.remove(parquet_path)


@task(log_prints=True)
def check_file():
    print(".env ", os.path.exists('.env'))
    print('~/env/gh_archive/.env ', os.path.exists('~/env/gh_archive/.env'))


@flow(name='load_file')
def etl_web_to_gcs(year=None, month=None, day=None, hours=None, data_path='../data', gcs_bucket='gh-archive'):
    # if env_file is not None:
    #     load_config(env_file)
    if year is None:
        current_time = datetime.now(pytz.utc) - timedelta(hours=1)
        year = current_time.year
        month = current_time.month
        day = current_time.day
        hours = current_time.hour
    json_path, parquet_path, gcs_path = get_savepath(data_path, year, month, day, hours)
    url = get_file_link(year, month, day, hours)
    get_file(url, json_path)
    preprocess_file(json_path, parquet_path)
    upload_to_gcs(gcs_bucket, parquet_path, gcs_path)
    remove_files(json_path, parquet_path)

if __name__ == '__main__':
    etl_web_to_gcs()