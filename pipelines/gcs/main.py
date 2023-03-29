from prefect import flow, task
import gzip
import json
from requests import get
from dotenv import load_dotenv
import os
import pandas as pd
from google.cloud import storage


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
    return os.path.join(os.getenv('DATA_PATH'), 'raw', f'{year}', f'{month:02d}', f'{year}-{month:02d}-{day:02d}-{hours}.json.gz')
    
    

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


@task(retries=3, log_prints=True)
def get_file(url, savepath):
    parent = os.path.dirname(savepath)
    if not os.path.exists(parent):
        os.makedirs(parent)
    with open(savepath, 'wb+') as file:
        response = get(url)
        file.write(response.content)
    
    with gzip.open(savepath, mode="rt") as f:
        data = [json.loads(line) for line in f]

    df = pd.DataFrame.from_records(data)
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%dT%H:%M:%SZ')
    print(len(df))
    print(df.head(1))
    print(df.dtypes)


@flow(name='load_file')
def load_file(year, month, day, hours=None, env_file='.env'):
    load_config(env_file)
    save_path = get_savepath(year, month, day, hours)
    url = get_file_link(year, month, day, hours)
    get_file(url, save_path)


if __name__ == '__main__':
    load_file(2023, 3, 26, 1)