# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2024,5,1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator', # 트리거 할 dag_id
        trigger_run_id=None, # 트리거된 dag 실행에 사용할 실행 id. 제공하지 않으면 실행 id 자동 생성
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False, # dag 실행 완료를 기다릴지 여부
                                   # true: 트리거 dag이 완료해야 연결된 main task도 완료 되어 다음 task 실행)
                                   # false: 트리거 dag 실행여부와 상관없이 연결된 main task가 완료되면 다음 task 진행 
        poke_interval=60, # 트리거 dag 실행 상태를 확인하는 간격(모니터 주기)
        allowed_states=['success'], # 트리거 dag이 어떤 상태로 끝나야 연결된 main task를 성공 처리 할지, 
                                    # fail도 넣어주면 트리거 dag이 success,fail 일때 연결된 main task 성공 처리
        failed_states=None # main task가 fail 처리를 하려면 트리거 dag이 어떤 상태여야 하는지 
        )

    start_task >> trigger_dag_task