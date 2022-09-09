import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from sodapy import Socrata
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args={
    'owner': 'keving91',
    'retries': 3,
    'retry_delay': timedelta(minutes= 5),
}


def extract_choques_nyc_from_api():
    client_choques_nyc = Socrata('data.cityofnewyork.us',
                  'P5yEsPDgBpkSlfs3G7Kg0DM6o',)
    results_choques_nyc = client_choques_nyc.get("h9gi-nx95", limit=100)
    results_df_choques_nyc = pd.DataFrame.from_records(results_choques_nyc)
    return results_df_choques_nyc.to_csv("/home/kevin/airflow/dataset_dump/Choques_NYC_test.csv")


def upload_master_to_s3():
    s3_hook = S3Hook(aws_conn_id= "AWS_S3_conn")
    s3_hook.load_file(
        filename= "/home/kevin/airflow/dataset_dump/Choques_NYC_test.csv",
        key = "Choques_NYC_test.csv",
        bucket_name = "proyecto-henry",
        replace= True)


with DAG(
    dag_id = 'AWS_S3_Upload',
    default_args = default_args,
    start_date = datetime(2022, 8, 30),
    schedule_interval = '@Monthly',
) as dag:


    task1 = PythonOperator(
                task_id = "extracting_choques_nyc_from_api",
                python_callable = extract_choques_nyc_from_api)


    task2 = PythonOperator(
                task_id = "uploading_master_to_s3",
                python_callable = upload_master_to_s3)


task1 >> task2