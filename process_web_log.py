from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Uche Nnodim',
    'start_date': days_ago(0),
    'email': ['uchejudennodim@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG

# define the DAG
dag = DAG(
    'process-web-log',
    default_args=default_args,
    description='This DAG processes web logs daily',
    schedule_interval=timedelta(days=1),
)

#define the first task
# define the tasks

extract = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/capstone/extracted_data.txt',
    dag=dag,
)

# define the second task
transform = BashOperator(
    task_id='transform_data',
    bash_command='cat /home/project/airflow/dags/capstone/extracted_data.txt | grep -Ev "198.46.149.143" > /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)

# define the task 'load'

load = BashOperator(
    task_id='load_data',
    bash_command='tar -czvf weblog.tar /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)
# task pipeline
extract >> transform >> load



# Summit a DAG
# cp process_web_log.py $AIRFLOW_HOME/dags
# airflow dags list
# airflow pause dag_id.
# airflow dags list|grep "process-web-log"
# airflow tasks list my-first-dag
# wget -P /home/project/airflow/dags/capstone https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/accesslog.txt