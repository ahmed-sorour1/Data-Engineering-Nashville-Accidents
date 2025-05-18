from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


DBT_project_path = '/nashville_accident'
spark_path = '/spark_preprocessing/spark_preprocessing.py'
snowflake_file_path = '/snowflake/download_from_snowflake.py'

with DAG(dag_id = 'nashville_accident_Spark_and_dbt', 
         description = 'DAG for Customers and their orders project from DBT',
         start_date = datetime(2025,5,17),
         schedule_interval = '10 * * * *',
         catchup = False, 
         dagrun_timeout = timedelta(minutes=20),
         tags = ['accident', 'nashville', 'spark', 'dbt']) as dag:
       
    task_deps = BashOperator(
        task_id = 'DBT_deps',
        bash_command = f'cd "{DBT_project_path}" && dbt deps'
)
   
    task_run = BashOperator(
        task_id = 'DBT_run',
        bash_command = f'cd "{DBT_project_path}" && dbt run'
)

    task_test = BashOperator(
        task_id = 'DBT_test',
        bash_command = f'cd "{DBT_project_path}" && dbt test'
)

    
    task_download_from_snowflake = BashOperator(
        task_id = 'download_from_snowflake',
        bash_command = f'python "{snowflake_file_path}"'
)

   
task_deps >> task_run >> task_test >> task_download_from_snowflake
    
