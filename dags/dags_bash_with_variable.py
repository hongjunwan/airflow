from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    var_value = Variable.get("sample_key") # 1안

    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable:{var_value}"
    )

    bash_var_2 = BashOperator(              # 2안, 2안을 권고함
        task_id="bash_var_2",
        bash_command="echo variave:{{var.value.sample_key}}"
    )