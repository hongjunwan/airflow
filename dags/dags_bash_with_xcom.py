from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command="echo START && "
                 "echo XCOM_PUSHED "
                 "{{ ti.xcom_push(key='bash_pushed',value='first_bush_message') }} &&"
                 "echo COMPLETE" # 마지막으로 출력한 문장은 return_value(키값)/COMPLETE(VALUE값)으로 저장됨
    )

    bash_pull = BashOperator(
    task_id='bash_pull',
    env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed')}}",
         'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push')}}"},
    bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
    do_xcom_push=False # XCOM에 올릴지 여부(AIRFLOW 사이트 DAG의 XCOM)
    )

    bash_push >> bash_pull