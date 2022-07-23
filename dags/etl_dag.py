import sys
import os
from airflow.models import DAG
from airflow.operators import bash
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

path = f"{os.path.abspath(os.path.dirname(__file__))}/src"
sys.path.append(path)

from src.etl import run_etl
from src.prepare_mail import send_holidays_for_the_month

dag_default_args = {
    'owner':'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
    'start_date': datetime(2022,1,1),
    'sla': timedelta(minutes=60)
}

with DAG(
    'etl_dag', 
    description='fetch and store in database',
    default_args=dag_default_args,
    schedule_interval='@monthly') as etl_dag:

    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl
    )

    # send mail and then log it in the database on a table that the mail has been sent with the time sent.
    mail_task = PythonOperator(task_id = 'mail_task', python_callable = send_holidays_for_the_month)

    # hard coding the email because it is just for me.
    log_mail_sent = PostgresOperator(
        task_id = 'log_mail_sent', 
        postgres_conn_id = 'postgres_calendarific_local',
        sql = "insert into log_email (sent_to, date_sent, time_sent) values ('adeniyikunle22@gmail.com', current_date, cast(localtime(0) as varchar)); commit;"
    )
    # query insert into log_email (sent_to, date_sent, time_sent) values ('adeniyikunle22@gmail.com', current_date, cast(localtime(0) as varchar));

    run_etl >> mail_task >> log_mail_sent



