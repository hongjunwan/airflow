from airflow import DAG
from airflow.decorators import task
import pendulum
import datetime

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=True,
) as dag:
    @task(task_id="python_task")
    def show_templates(**kwargs):
        from pprint import pprint #print문과 동일한데 이쁘게 출력 해주는 print문
        pprint(kwargs)

    show_templates()