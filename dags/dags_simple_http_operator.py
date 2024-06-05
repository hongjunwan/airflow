# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2024, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='CON_TEST', # 생성한 connections id 입력
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',
                 # 인증키 직접 입력하지 않고 apikey_openapi_seoul_go_kr 변수에 인증키 작성
                 # 1/10: 1행~10행 까지
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info') # 해당 task_ids의 return 값 가져옴
        import json
        from pprint import pprint

        pprint(json.loads(rslt)) # loads(): json 문자열을 파이썬 객체로 변환, http 요청의 전문을 읽을때 많이 사용
        
    tb_cycle_station_info >> python_2()