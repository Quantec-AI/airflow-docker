from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from git import Repo, Git
import os
import nltk

from datetime import datetime, date
from dateutil import tz

from ms_teams_webhook_operator import MSTeamsWebhookOperator

items = {'US':['US','Topics'],'KR':['KR','Equity']}

analyzer_us_str = f"cd /opt/devs/Q_SA_Int && python analyzers/news_analyzer.py --region={items.get('US')[0]} --product_type={items.get('US')[1]}"
analyzer_kr_str = f"cd /opt/devs/Q_SA_Int && python analyzers/news_analyzer.py --region={items.get('KR')[0]} --product_type={items.get('KR')[1]}"

with DAG(
    dag_id="analyzer",
    start_date=datetime(2022,11,30,tzinfo=tz.gettz('Asia/Seoul')),
    schedule="40 16 * * 3") as dag:

    @task
    def preload():
        nltk.download('punkt')

    run_analyzer_us = BashOperator(task_id='run_analyzer_us', bash_command=analyzer_us_str)
    run_analyzer_kr = BashOperator(task_id='run_analyzer_kr', bash_command=analyzer_kr_str)

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

        try:
            # Stage files
            repo.index.add(file_paths)
            # Commit files
            repo.index.commit(msg)

            # Pull & Push
            repo.remotes.origin.pull()
            repo.remotes.origin.push()

        except:
            msg = 'Git Sync Failed'

        return msg

    teams_msg = MSTeamsWebhookOperator(task_id='ms_teams_msg',
        http_conn_id='msteams_webhook_url',
        message = "Git Sync Result",
        subtitle = "{{ ti.xcom_pull(task_ids='git_sync') }}",
        theme_color = "00FF00",
        button_text = "GitHub",
        button_url = "https://github.com/Quantec-AI/Q_SA_Int")


    preload() >> run_analyzer_us >> run_analyzer_kr >> git_sync() >> teams_msg
