from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label # label 라이브러리 등록


with DAG(
    dag_id="dags_empty_with_edge_label",
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    empty_1 = EmptyOperator(
        task_id='empty_1'
    )

    empty_2 = EmptyOperator(
        task_id='empty_2'
    )

    empty_1 >> Label('1과 2사이') >> empty_2

    empty_3 = EmptyOperator(
        task_id='empty_3'
    )

    empty_4 = EmptyOperator(
        task_id='empty_4'
    )

    empty_5 = EmptyOperator(
        task_id='empty_5'
    )

    empty_6 = EmptyOperator(
        task_id='empty_6'
    )
    
    # empty_2와 연결되는 empty_3, empty_4, empty_5 연결선 모두에 comment 등록됨
    empty_2 >> Label('Start Branch') >> [empty_3,empty_4,empty_5] >> Label('End Branch') >> empty_6