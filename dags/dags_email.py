from airflow import DAG
import pendulum
import datetime
from datetime import timedelta
from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email",
    schedule=timedelta(seconds=30),
    start_date= pendulum.datetime (2024, 5, 29, tz="Asia/Seoul"),
    catchup=True
) as dag:
    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice # choice 사용위한 import 등록
        return choice(['Success','Fail']) # 랜덤결과 반환
    
    send_email = EmailOperator(
        task_id='send_email',
        to='wjdghks29@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul")|ds }} 마계후니다!',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul")|ds }} 경보경보!! <br> \
                      후니가 집에 침투했습니다!!<br>' # <br> : 줄바꿈    
    )

    some_logic() >> send_email 