from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta, date

processing_date = date.today().strftime("%Y-%m-%d")

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
        endpoint=f"/send_data_by_date?date={processing_date}",  # Gửi date qua query
        method='POST',  # POST vẫn hợp lệ nếu không có body
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
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


    send_data_to_hdfs >> process_data_with_spark
