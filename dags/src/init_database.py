import os

from sqlalchemy import engine, text
from dotenv import load_dotenv
from psycopg2._psycopg import connection

load_dotenv()

# initialize db connection
DB_NAME = os.environ.get('DB_NAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_USER = os.environ.get('DB_USER')
HOSTNAME = os.environ.get('HOSTNAME')
PORT = 5432 if os.environ.get('PORT') == None else os.environ.get('PORT') # ternary operator
CONNECTION_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{HOSTNAME}:{PORT}/{DB_NAME}"

print(CONNECTION_STRING)
# engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')
# db_engine = engine.create_engine('postgresql+psycopg2://calendarific:calendarific@localhost/calendarific')
db_engine = engine.create_engine(CONNECTION_STRING)

def run_query(query, engine=db_engine):
    try:
        with db_engine.connect() as connection:
            response = connection.execute(str(query), multi=True)
            print(f"{str(query)} executed succesfully")
            return response
    except Exception as error:
        print(str(query))
        print(f"Error is {str(error)}")

def run_select_query(query, engine=db_engine):
    try:
        with db_engine.connect() as connection:
            response = connection.execute(text(str(query)), multi=True) # difference here (text)
            print(f"{str(query)} executed succesfully")
            
            if response.rowcount == 0:
                print("No rows fetched")
                return []

            return [row for row in response]
    except Exception as error:
        print(str(query))
        print(f"Error is {str(error)}")

def run_parameterized_select_queries(query, params, engine=db_engine):
    try:
        with db_engine.connect() as connection:
            response = connection.execute(text(str(query)), params, multi=True) # difference here (text)
            print(f"{str(query)} executed succesfully")
            
            if response.rowcount == 0:
                print("No rows fetched")
                return []

            return [row for row in response]
            # return response
    except Exception as error:
        print(str(query))
        print(f"Error is {str(error)}")
