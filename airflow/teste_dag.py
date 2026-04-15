from airflow import DAG
from datetime import datetime

with DAG('teste', start_date=datetime(2026, 4, 1), schedule_interval='@daily', catchup=False) as dag:
 