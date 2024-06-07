from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator # 해당 경로에 SeoulApiToCsvOperator class 사용
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024,5,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 코로나19 확진자 발생동향'''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        # opt는 workspace 경로, files 폴더 생성하고 yaml 파일에서 연결해줘야함, data_interval 부분은 배치종료일자별 폴더 만들기 위해서 사용
        file_name='TbCorona19CountStatus_{{data_interval_end.in_timezone("Asia/Seoul") + macros.timedelta(hours=8) | ds_nodash }}.csv' # 해당명으로 파일 생성
    )
    
    '''서울시 코로나19 백신 예방접종 현황'''
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id='tv_corona19_vaccine_stat_new',
        dataset_nm='tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew_{{data_interval_end.in_timezone("Asia/Seoul") + macros.timedelta(hours=8) | ds_nodash }}.csv'
    )

    tb_corona19_count_status >> tv_corona19_vaccine_stat_new