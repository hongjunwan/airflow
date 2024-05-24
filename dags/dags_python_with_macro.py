from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # macro 사용
    @task(task_id='task_using_macro',
          templates_dict={'start_date':'{{(data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1))|ds}}',
                          'end_date':'{{(data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1))|ds}}'
          } # templates_dict는 키값이고 start,end 는 딕셔너리
            # templates_dict가 키값이 되고 def의 **kwargs로 templates_dict의 값(start_date, end_date) 전체가 전달됨
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {} # templates_dict키값의 딕셔너리 전체가 templates_dict 변수로 할당됨 없으면 {}
        if templates_dict: # templates_dict키 값에 대한 정의
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)

    # macro 미사용 직접 구현
    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs['data_interval_end'] #kwargs의 defult키값 data_interval_end 값을 꺼내옴
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()