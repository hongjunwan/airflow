from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg1',
    start_date=pendulum.datetime(2024,5,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    ) # task1

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')
    # task2, raise 구문 : 실행 결과를 실패로 만듬 

    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상 처리') # task3

    @task(task_id='python_downstream_1', trigger_rule='all_done')
    def python_downstream_1():
        print('정상 처리')
    # task4, trigger 하위조건 생성 all_done : task1,task2,task3이 모두 성공하면 실행

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()