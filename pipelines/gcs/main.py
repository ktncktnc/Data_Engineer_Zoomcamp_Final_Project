from prefect import flow, task
import gzip
import json
from requests import get
from dotenv import load_dotenv
import os
import pandas as pd
from google.cloud import storage


def mkdir_if_not_exist(path):
    if not os.path.exists(path):
        os.makedirs(path)

@task(retries=1)
def load_config(path):
    load_dotenv(path)


@task(retries=1)
def get_savepath(year, month, day, hours):
    if hours is None:
        hours = ''
    elif type(hours) == list:
        hours = '{' + str(hours[0]) + '..' + str(hours[1]) + '}'
    else:
        hours = str(hours)
    json_gz_path = os.path.join(os.getenv('DATA_PATH'), 'raw', f'{year}', f'{month:02d}', f'{year}-{month:02d}-{day:02d}-{hours}.json.gz')
    parquet_path = os.path.join(os.getenv('DATA_PATH'), 'processed', f'{year}', f'{month:02d}', f'{year}-{month:02d}-{day:02d}-{hours}.parquet')
    return json_gz_path, parquet_path
    

@task(retries=1)
def get_file_link(year, month, day, hours=None):
    link = 'https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz'
    if hours is None:
        hours = ''
    elif type(hours) == list:
        hours = '{' + str(hours[0]) + '..' + str(hours[1]) + '}'
    else:
        hours = str(hours)
    link = link.format(year=year, month=month,day=day, hour=hours)
    return link


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
    df.to_parquet(parquet_path)


@task(retries=3)
def upload_to_gcs(parquet_path):
    client = storage.Client()
    bucket = client.get_bucket(os.getenv('GCS_BUCKET'))
    blob = bucket.blob(parquet_path)
    blob.upload_from_filename(parquet_path)


@flow(name='load_file')
def load_file(year, month, day, hours=None, env_file='.env'):
    load_config(env_file)
    json_path, parquet_path = get_savepath(year, month, day, hours)
    url = get_file_link(year, month, day, hours)
    get_file(url, json_path)
    preprocess_file(json_path, parquet_path)
    upload_to_gcs(parquet_path)


if __name__ == '__main__':
    load_file(2023, 3, 26, 1)