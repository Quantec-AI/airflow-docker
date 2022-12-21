from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from pendulum import datetime

from scrapy_intervals import KREquityScrapyTimetable

scrapy_init_str = f'cd /opt/devs/Q_SA-airflow/scrapy/Q_SA_Init && scrapy crawl Q_SA_Init -a region=KR -a product_type=Equity'
scrapy_news_str = f'cd /opt/devs/Q_SA-airflow/scrapy/Q_SA_News && scrapy crawl Q_SA_News -a region=KR -a product_type=Equity -a lookback_start=10'
scrapy_price_str = f'cd /opt/devs/Q_SA-airflow/scrapy/Q_SA_Price && scrapy crawl Q_SA_Price -a region=KR -a product_type=Equity'
scrapy_forum_str = f'cd /opt/devs/Q_SA-airflow/scrapy/Q_SA_Forum && scrapy crawl Q_SA_Forum -a region=KR -a product_type=Equity -a lookback_start=10'

with DAG(
    dag_id="scrapy_kr_equity-airflow",
    start_date=datetime(2022,12,5,tz='UTC'),
    timetable=KREquityScrapyTimetable(),
    catchup=False) as dag:

    run_scrapy_init = BashOperator(task_id='run_scrapy_init', bash_command=scrapy_init_str)
    run_scrapy_news = BashOperator(task_id='run_scrapy_news', bash_command=scrapy_news_str)
    run_scrapy_price = BashOperator(task_id='run_scrapy_price', bash_command=scrapy_price_str)
    run_scrapy_forum = BashOperator(task_id='run_scrapy_forum', bash_command=scrapy_forum_str)

run_scrapy_init >> [run_scrapy_news, run_scrapy_price, run_scrapy_forum]