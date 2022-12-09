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
    schedule="40 16 * * 1-5") as dag:

    @task
    def preload():
        nltk.download('punkt')

    do_preload = preload()

    run_analyzer_us = BashOperator(task_id='run_analyzer_us', bash_command=analyzer_us_str)
    run_analyzer_kr = BashOperator(task_id='run_analyzer_kr', bash_command=analyzer_kr_str)

    @task
    def git_sync():
        base_dir = '/opt/devs/Q_SA_Int/'
        data_base_dir = base_dir + 'data/'
        data_items = ['current/','news/','price/']
        data_dirs = list(map(lambda x:os.path.join(data_base_dir,x),data_items))

        # Files to commit
        file_paths = [os.path.join(data_dir,x.name) for data_dir in data_dirs for x in os.scandir(data_dir)]
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

    sync_msg = git_sync()

    @task.branch(task_id='branching')
    def msg_branching(ti=None):
        xcom_value = ti.xcom_pull(task_ids='git_sync')
        if xcom_value != 'Git Sync Failed':
            return 'ms_teams_success_msg'
        else:
            return 'ms_teams_fail_msg'

    do_branching = msg_branching()

    teams_success_msg = MSTeamsWebhookOperator(task_id='ms_teams_success_msg',
        http_conn_id='msteams_webhook_url',
        message = "Git Sync Result",
        subtitle = "{{ ti.xcom_pull(task_ids='git_sync') }}",
        theme_color = "00FF00",
        button_text = "GitHub",
        button_url = "https://github.com/Quantec-AI/Q_SA_Int")

    teams_fail_msg = MSTeamsWebhookOperator(task_id='ms_teams_fail_msg',
        http_conn_id='msteams_webhook_url',
        message = "Git Sync Result",
        subtitle = "{{ ti.xcom_pull(task_ids='git_sync') }}",
        theme_color = "00FF00")

do_preload >> run_analyzer_us >> run_analyzer_kr >> sync_msg >> do_branching >> [teams_success_msg, teams_fail_msg]
