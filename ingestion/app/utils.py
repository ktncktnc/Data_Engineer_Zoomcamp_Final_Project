from dotenv import load_dotenv
import os


def mkdir_if_not_exist(path):
    if not os.path.exists(path):
        os.makedirs(path)


def load_config(path):
    load_dotenv(path)   

def get_savepath(data_path, year, month, day, hours):
    if hours is None:
        hours = ''
    elif type(hours) == list:
        hours = '{' + str(hours[0]) + '..' + str(hours[1]) + '}'
    else:
        hours = str(hours)
    json_gz_path = os.path.join(data_path, 'raw', f'{year}', f'{month:02d}', f'{year}-{month:02d}-{day:02d}-{hours}.json.gz')
    parquet_path = os.path.join(data_path, 'processed', f'{year}', f'{month:02d}', f'{year}-{month:02d}-{day:02d}-{hours}.parquet')
    gcs_path = os.path.join('processed', f'{year}', f'{month:02d}', f'{year}-{month:02d}-{day:02d}-{hours}.parquet')
    return json_gz_path, parquet_path, gcs_path


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
