from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2024,5,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing
        #closing(): DB 서버와의 연결(session)
        # client(worker)-session(cursor)-db서버(컨테이너)
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);' # 실제 수행 sql
                cursor.execute(sql,(dag_id,task_id,run_id,msg)) # %s에 차례대로 dag_id, task_id, run_id, msg 값 매핑
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5432', 'junwan', 'junwan', 'junwan'] # insrt_postgres 함수에 전달 값
    )
        
    insrt_postgres