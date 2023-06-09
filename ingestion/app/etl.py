from bigquery import etl_gcs_to_bigquery
from gcs import etl_web_to_gcs
from prefect import flow


@flow
def etl_data_to_big_query(year=None, month=None, day=None, hours=None, data_path='../data', gcs_bucket='gh-archive'):
    etl_web_to_gcs(year, month, day, hours, data_path, gcs_bucket)
    etl_gcs_to_bigquery(year, month, day, hours, data_path, gcs_bucket)


if __name__ == '__main__':
    etl_data_to_big_query()
