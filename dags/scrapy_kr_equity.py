from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from pendulum import datetime

from scrapy_intervals import KREquityScrapyTimetable

scrapy_str = f'cd /opt/devs/Q_SA_Int && scrapy crawl Q_SA -a region=KR -a product_type=Equity -a lookback_start=10'
run_scrapy = BashOperator(task_id='run_scrapy', bash_command=scrapy_str)

with DAG(
    dag_id="scrapy_kr_equity",
    start_date=datetime(2022,12,5,tz='UTC'),
    timetable=KREquityScrapyTimetable(),
    catchup=False) as dag:

    run_scrapy