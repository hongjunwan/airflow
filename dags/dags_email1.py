from airflow import DAG
import pendulum
import datetime
from datetime import timedelta
from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_hoony",
    schedule=timedelta(seconds=10),
    start_date= pendulum.datetime (2024, 5, 29, tz="Asia/Seoul"),
    catchup=True
) as dag:
    
    send_email = EmailOperator(
        task_id='send_email',
        to='wjdghks29@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul")|ds }} 마계망후니다!',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul")|ds }} 경보경보!! <br> \
                      후니가 집에 침투했습니다!!<br>' # <br> : 줄바꿈    
    )

    send_email 