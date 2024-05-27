from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dag_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_id='python_xcom_push_by_return') #해당 task_id가 return 한 값 가져옴
        print('xcom_pull 매서드로 직접 찾은 리턴 값:' + value1)  

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)
          
    
    python_xcom_push_by_return = xcom_push_result() # return값 Success를 python_xcom_push_by_return 함수에 들어간 것처럼 만듬
    xcom_pull_2(python_xcom_push_by_return) # Success 값이 status로 들어감
    python_xcom_push_by_return >> xcom_pull_1()