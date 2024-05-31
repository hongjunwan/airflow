from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id='dags_branch_python_operator',
    start_date=pendulum.datetime(2024,5,1, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False
) as dag:
    def select_random():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a' # 2. random 값이 A이면 task_a 실행
                            # 단일로 실행시키고 싶은 경우 string 타입으로 task_id 작성
        elif selected_item in ['B','C']:
            return ['task_b','task_c'] # 2. random 값이 b 또는 c이면 task_b, task_c 실행
                                       # 실행시키고 싶은 task가 여러개면 list로 task_id를 작성
    
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random
    ) # 1. select_random() 실행 
    
    def common_func(**kwargs):
        print(kwargs['selected']) # 4. 결과 출력

    # 3. 2번 결과에 따라 task별 common_Func 실행
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]