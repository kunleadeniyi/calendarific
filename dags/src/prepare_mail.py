# from main import engine
from sqlalchemy import sql
from src.init_database import run_query, run_select_query, run_parameterized_select_queries,  db_engine
from src.mailer import prepare_content, prepare_message, send_mail
from datetime import datetime

import pprint

# params = {'country_code': 'NG', }
# sql_query = "select * from calendarific.holidays where country_code = :country_code;"
# result = run_parameterized_select_queries(engine=db_engine, query=sql_query, params=params)

def get_holidays_for_current_month(current_month):
    sql_query =  f"select * from calendarific.holidays where date_part('month', hol_date) = {current_month} and country_code = 'NG';"
    dec_holidays = run_select_query(sql_query)
    return dec_holidays

def send_holidays_for_the_month():

    holidays = get_holidays_for_current_month(datetime.today().month)
    message_content = prepare_content(holidays)
    #print(message_content)
    email = 'adeniyikunle22@gmail.com'
    subject = f'{datetime.now().strftime("%B")} Holidays'
    msg = prepare_message(email, subject, str(message_content), message_content)

    send_mail(msg)

print(datetime.today().month)

