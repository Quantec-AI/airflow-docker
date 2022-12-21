from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from pendulum import datetime

scrapy_news_str = f'cd /opt/devs/Q_SA-airflow/scrapy/Q_SA_News && scrapy crawl Q_SA_News -a region=US -a product_type=General -a lookback_start=10'

with DAG(
    dag_id="scrapy_us_topics_cnbc-airflow",
    start_date=datetime(2022,12,5,tz='UTC'),
    schedule="00 11 * * 1-5",
    catchup=False) as dag:

    run_scrapy_news = BashOperator(task_id='run_scrapy_news', bash_command=scrapy_news_str)

run_scrapy_news