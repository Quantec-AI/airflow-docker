from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

import nltk
from pendulum import datetime

with DAG(
    dag_id="analyzer",
    start_date=datetime(2022,12,5,tz='Asia/Seoul'),
    schedule="40 16 * * 3") as dag:

    @task
    def preload():
        nltk.download('punkt')

    items = {'US':['US','Topics'],'KR':['KR','Equity']}

    analyzer_us_str = f"cd /opt/devs/Q_SA_Int && python analyzers/news_analyzer.py --region={items.get('US')[0]} --product_type={items.get('US')[1]}"
    analyzer_kr_str = f"cd /opt/devs/Q_SA_Int && python analyzers/news_analyzer.py --region={items.get('KR')[0]} --product_type={items.get('KR')[1]}"

    run_analyzer_us = BashOperator(task_id='run_analyzer_us', bash_command=analyzer_us_str, dag=dag)
    run_analyzer_kr = BashOperator(task_id='run_analyzer_kr', bash_command=analyzer_kr_str, dag=dag)

    preload() >> run_analyzer_us >> run_analyzer_kr