from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta, date
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

processing_date = date.today().strftime("%Y-%m-%d")
# processing_date = '2024-06-28'

def branch_decision(**kwargs):
    ti = kwargs['ti']
    task_state = ti.xcom_pull(task_ids='send_data_to_hdfs', key='return_value')
    if task_state:
        return 'process_data_with_spark'
    else:
        return 'skip_task'

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 31),
    "email": ["dangptpt@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": 60,
}
with DAG(
    dag_id='bicycle_data_pipeline',
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=False,
    tags=['bicycle_data', 'hdfs', 'spark', 'postgres'],
) as dag:
    send_data_to_hdfs = SimpleHttpOperator( 
        task_id='send_data_to_hdfs',
        http_conn_id='data_ingestion_service',
        endpoint=f"/send_data_by_date?date={processing_date}", 
        method='POST',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    branch_check = BranchPythonOperator(
        task_id='branch_check',
        python_callable=branch_decision,
        provide_context=True,
    )
    
    process_data_with_spark = SparkSubmitOperator(
        task_id='process_data_with_spark',
        application='/opt/spark/app/app.py',  
        conn_id='spark_default',
        executor_cores=2,
        executor_memory='1g',
        name='bicycle_data_processing',
        jars='/opt/spark/jars/postgresql-42.7.4.jar',
        verbose=True,
    )
    skip_task = DummyOperator(task_id='skip_task')


    send_data_to_hdfs >> branch_check
    branch_check >> [process_data_with_spark, skip_task]
