from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dag_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value="value_1")
        ti.xcom_push(key='result2', value=[1,2,3])

    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value="value_2")
        ti.xcom_push(Key='result2', value=[1,2,3,4])  

    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key="result1")
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)  
    
    xcom_push1() >> xcom_push2() >> xcom_pull()
    #xcom_pull()에서 value1 값으로 pull해오는 result1의 값이
    #task명이 명시되지 않아 push1,push2 중 어느 값을 가져오는지
    #확인필요 -> 순서상 push2가 뒤에 돌기때문에 push2의 값으로 가져옴