from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_python_email_operator",
    schedule="1/60 * * * *",
    start_date= pendulum.datetime (2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice # choice 사용위한 import 등록
        return choice(['Success','Fail']) # 랜덤결과 반환
    
    send_email = EmailOperator(
        task_id='send_email',
        to='dhks223@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul")|ds }} 안녕!',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul")|ds }} 처리 결과는 <br> \
                      {{ ti.xcom_pull(task_ids="something_task")}} 했습니다 <br>' # <br> : 줄바꿈    
    )

    some_logic() >> send_email 