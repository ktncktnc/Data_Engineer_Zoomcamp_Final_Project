from prefect import flow, task
import gzip
import json
from requests import get
import os
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
def upload_to_gcs(parquet_path, gcs_path):
    gcs_block = GcsBucket.load(os.getenv('GCS_BUCKET'))
    gcs_block.upload_from_path(from_path=parquet_path, to_path=gcs_path)


@task(retries=1)
def remove_files(json_path, parquet_path):
    os.remove(json_path)
    os.remove(parquet_path)


@flow(name='load_file')
def etl_web_to_gcs(year, month, day, hours=None, env_file='.env'):
    load_config(env_file)
    json_path, parquet_path, gcs_path = get_savepath(year, month, day, hours)
    url = get_file_link(year, month, day, hours)
    get_file(url, json_path)
    preprocess_file(json_path, parquet_path)
    upload_to_gcs(parquet_path, gcs_path)
    remove_files(json_path, parquet_path)


if __name__ == '__main__':
    etl_web_to_gcs(2023, 3, 26, 1)