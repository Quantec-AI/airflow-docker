from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from pendulum import datetime

from scrapy_intervals import USTopicsScrapyTimetable

scrapy_str = f'cd /opt/devs/Q_SA_Int && scrapy crawl Q_SA -a region=US -a product_type=Topics -a lookback_start=10'

with DAG(
    dag_id="scrapy_us_topics",
    start_date=datetime(2022,12,5,tz='UTC'),
    timetable=USTopicsScrapyTimetable(),
    catchup=False) as dag:

    run_scrapy = BashOperator(task_id='run_scrapy', bash_command=scrapy_str)

run_scrapy