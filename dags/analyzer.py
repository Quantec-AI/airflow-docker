from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from git import Repo, Git
import os
import nltk

from datetime import datetime, date
from dateutil import tz

with DAG(
    dag_id="analyzer",
    start_date=datetime(2022,11,30,tzinfo=tz.gettz('Asia/Seoul')),
    schedule="40 16 * * 3") as dag:

    @task
    def preload():
        nltk.download('punkt')

    items = {'US':['US','Topics'],'KR':['KR','Equity']}

    analyzer_us_str = f"cd /opt/devs/Q_SA_Int && python analyzers/news_analyzer.py --region={items.get('US')[0]} --product_type={items.get('US')[1]}"
    analyzer_kr_str = f"cd /opt/devs/Q_SA_Int && python analyzers/news_analyzer.py --region={items.get('KR')[0]} --product_type={items.get('KR')[1]}"

    run_analyzer_us = BashOperator(task_id='run_analyzer_us', bash_command=analyzer_us_str, dag=dag)
    run_analyzer_kr = BashOperator(task_id='run_analyzer_kr', bash_command=analyzer_kr_str, dag=dag)

    @task
    def git_sync():
        base_dir = '/opt/devs/Q_SA_Int/'
        data_dir = base_dir + 'data/current/'

        # Files to commit
        file_paths = list(map(lambda x:os.path.join(data_dir,x.name),os.scandir(data_dir)))
        print(file_paths)

        # Initialize GitHub
        repo = Repo(base_dir)

        # Commit msg
        msg = f"Analyzed data as of {date.today().strftime('%Y-%m-%d')} Uploaded"
        # Stage files
        repo.index.add(file_paths)
        # Commit files
        repo.index.commit(msg)

        # Pull & Push
        repo.remotes.origin.pull()
        repo.remotes.origin.push()

    preload() >> run_analyzer_us >> run_analyzer_kr >> git_sync()