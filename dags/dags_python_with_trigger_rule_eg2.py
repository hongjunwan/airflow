from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg2',
    start_date=pendulum.datetime(2024,5,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    @task.branch(task_id='branching')
    def random_branch():
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'
    # task1, random 값 결과에 따라 해당하는 task 실행
    # 실행되는 task외엔 모두 skip 처리됨

    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    ) # task2 

    @task(task_id='task_b')
    def task_b():
        print('정상 처리')
    # task3

    @task(task_id='task_c')
    def task_c():
        print('정상 처리')
    # task4

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상 처리')
    # task5, trigger 하위조건 생성 none_skipped : task2,task3,task4가 모두 스킵하지 않아야 실행

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()