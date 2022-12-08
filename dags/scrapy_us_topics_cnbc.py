from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from pendulum import datetime

scrapy_str = f'cd /opt/devs/Q_SA_Int && scrapy crawl Q_SA -a region=US -a product_type=General -a lookback_start=10'
run_scrapy = BashOperator(task_id='run_scrapy', bash_command=scrapy_str)

with DAG(
    dag_id="scrapy_us_topics_cnbc",
    start_date=datetime(2022,12,5,tz='Asia/Seoul'),
    schedule="00 11 * * 0-5") as dag:

    run_scrapy