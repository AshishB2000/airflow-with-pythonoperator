from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from dags.load_code import load_function
from dags.Extract_code import extract_function
from dags.tansform_code import transform_function

default_args={
    'owner' : 'Ashish',
    'start_date' : datetime(2024,5,24),
    'retry_delay' : timedelta(minutes=5),
    'retries' : 0x1
}

dag= DAG(

    dag_id= 'etl1',
    default_args= default_args,
    schedule_interval= None,
    catchup=False,
    tags=['etl1_with_python','etl_demo']
)

start= DummyOperator(task_id="start", dag=dag)

extrat= PythonOperator(
    task_id="extract",
    python_callable=extract_function,
    dag=dag)
transform= PythonOperator(
    task_id="transform",
    python_callable=transform_function,
    dag=dag,
    op_args=["arguments for the transform"])

load= PythonOperator(
    task_id="load",
    python_callable=load_function,
    dag=dag,
    op_kwargs={"l2":"arguments 2 for the load", "l1": "arguments 1 for the load"})

end= DummyOperator(task_id="end", dag=dag)



start >> extrat >> transform  >> load >> end
