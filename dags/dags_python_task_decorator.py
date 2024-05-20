
from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_python_task_deorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    @task(task_id="python_task")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task decorator1 실행')
    python_task_1 = print_context('task decorator2 실행')
    python_task_1 = print_context('task decorator3 실행')    
