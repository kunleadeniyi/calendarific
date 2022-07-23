from src.prepare_mail import send_holidays_for_the_month
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner':'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
        'send_mail_dag',
        start_date = datetime(2022,4,1),
        schedule_interval = '@monthly',
        default_args = default_args,
        catchup = False
    ) as send_mail_dag:


    # send mail and then log it in the database on a table that the mail has been sent with the time sent.
    mail_task = PythonOperator(task_id = 'mail_task', python_callable = send_holidays_for_the_month)

    # hard coding the email because it is just for me.
    log_mail_sent = PostgresOperator(
        task_id = 'log_mail_sent', 
        postgres_conn_id = 'postgres_calendarific_local',
        sql = "insert into log_email (sent_to, date_sent, time_sent) values ('adeniyikunle22@gmail.com', current_date, cast(localtime(0) as varchar)); commit;"
    )
    # query insert into log_email (sent_to, date_sent, time_sent) values ('adeniyikunle22@gmail.com', current_date, cast(localtime(0) as varchar));

    mail_task >> log_mail_sent

