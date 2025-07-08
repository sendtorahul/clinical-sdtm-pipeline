from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="sdtm_pipeline",
         start_date=datetime(2024, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    transform_dm = BashOperator(
        task_id="transform_dm",
        bash_command="spark-submit /opt/airflow/dags/glue_jobs/transform_dm.py"
    )

    transform_dm
