import os

import pandas as pd

import requests
import json
# import calendarific
from sqlalchemy import create_engine, exc, MetaData
from init_database import db_engine 
#init_database import db_engine
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY = os.environ.get('API_KEY')
todays_date = datetime.today()

# initialize db connection
# DB_NAME = os.environ.get('DB_NAME')
# DB_PASSWORD = os.environ.get('DB_PASSWORD')
# DB_USER = os.environ.get('DB_USER')
# HOSTNAME = os.environ.get('HOSTNAME')
# CONNECTION_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{HOSTNAME}/{DB_NAME}"
# db_engine = create_engine(CONNECTION_STRING)

# # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')
# engine = create_engine('postgresql+psycopg2://calendarific:calendarific@localhost/calendarific')

meta = MetaData()
meta.reflect(bind=db_engine)

# Get holiday url
base_url = 'https://calendarific.com/api/v2/holidays?'
# Get country data url
country_url = 'https://calendarific.com/api/v2/countries?'

def get_country_info(url):
    """
    url: (string) country_url global variable

    returns a list of lists with each inner list having country code and country name
    """
    req_params = {'api_key': API_KEY}
    country_response = requests.get(url, params=req_params)
    country_dict = country_response.json()
    print(country_dict)
    if (len(country_dict['response']['countries']) < 1):
        return []
    else:
        countries = [[country['iso-3166'], country['country_name']] for country in country_dict['response']['countries']]
        return countries

def get_country_code_list(db_engine):
    """
    db_engine: (sqlalchemy.engine.base.Engine)

    returns: (list) list of country codes
    """
    with db_engine.connect() as connection:
        country_codes = connection.execute('SELECT country_code from calendarific.countries;')
        country_list = [row['country_code'] for row in country_codes]
    return country_list

def get_holiday_per_country(url, req_params):
    """
    url: (string) base_url global variable
    req_params: (dictionary) dictionary containing {api_key: API_KEY, country: country_code, year: year}

    returns a list of dictionaries with holiday information for a single country
    """
    holiday_response = requests.get(url, params=req_params)
    holiday_dict = holiday_response.json()
    # print(len(holiday_dict['response']))
    if len(holiday_dict['response']) < 1:
        return []
    else: 
        holidays = holiday_dict['response']['holidays']
        return holidays

def get_holidays_for_nigeria(url):
    """
    url: (string) base_url global variable

    returns a list of dictionaries with holiday information for a single country
    """
    req_params = {
        'api_key': API_KEY,
        'country': 'NG',
        'year': todays_date.year
    }
    nigerian_holidays = get_holiday_per_country(url, req_params)
    return nigerian_holidays

def get_holiday_for_all_countries(url):
    """
    url: (string) base_url global variable
    
    returns a list of dictionaries with holiday information for all countries
    Should be passed to the create_holiday_datafame()
    """
    combined_holiday_list = []
    country_list = get_country_code_list(db_engine)
    for country in country_list:
        req_params = {
        'api_key': API_KEY,
        'country': country,
        'year': todays_date.year
        }
        temp_list = get_holiday_per_country(url, req_params)
        combined_holiday_list += temp_list
    
    return combined_holiday_list

def create_holiday_datafame(holidays):
    """
    holidays: (list) list of objects gotten from get_holiday_for_all_countries()

    returns: pandas dataframe
    """
    if len(holidays) == 0:
        df_holiday = pd.DataFrame(columns = ['holiday', 'description', 'country_code', 'hol_date', 'year', 'hol_type', 'hol_type_2', 'states'])
        return df_holiday
    else:
        hol_list = []
        description = []
        country = []
        hol_date = []
        year = []
        hol_type = []
        hol_type_2 = []
        states = []

        for holiday in holidays:
            hol_list.append(holiday['name'])
            description.append(holiday['description'])
            country.append(holiday['country']['id'].upper())
            hol_date.append(holiday['date']['iso'])
            year.append(holiday['date']['datetime']['year'])
            hol_type.append(holiday['type'][0])
            hol_type_2.append(holiday['type'][1] if len(holiday['type']) > 1 else None)
            states.append(holiday['locations'] if isinstance(holiday['states'], list) else holiday['states'])

        # create dataframe
        dict = {
            'holiday': hol_list,
            'description': description,
            'country_code': country,
            'hol_date': hol_date,
            'year': year,
            'hol_type': hol_type,
            'hol_type_2': hol_type_2,
            'states': states
        }
        df_holiday = pd.DataFrame(dict)
        return df_holiday

def create_country_dataframe(countries):
    """
    countries: (list) list of objects gotten from get_holiday_for_all_countries()

    returns: pandas dataframe
    """

    if len(countries) == 0:
        df_countries = pd.DataFrame(columns= ['country_code', 'country'])
        return df_countries
    else:
        df_countries = pd.DataFrame(countries, columns=['country_code', 'country'])
        return df_countries

# Insert to database
def insert_countries(dataframe):
    
    # using pandas to_sql method
    # dataframe.to_sql('countries', schema='calendarific', con = db_engine, if_exists = 'append', chunksize = 10)

    countries_table = meta.tables['countries']
    print(countries_table)
    inserted_country_count = 0
    insert_error_count = 0
    for row in dataframe.itertuples():
        try:
            insert_statement = countries_table.insert().values(
                country_code = row.country_code,
                country = row.country
            )

            with db_engine.connect() as connection:
                connection.execute(insert_statement)
            
            inserted_country_count += 1
        except exc.IntegrityError as error:
            insert_error_count += 1
        except Exception as e:
            insert_error_count += 1

    print(f"{insert_error_count} errors encountered")
    print(f"{inserted_country_count} inserted countries")   
    

def insert_holidays(dataframe):
    holidays_table = meta.tables['holidays']
    duplicate_counter = 0
    other_err_counter = 0
    inserted_rows = 0
    for row in dataframe.itertuples():
        try:
            insert_statement = holidays_table.insert().values(
                holiday= row.holiday,
                description= row.description,
                country_code= row.country_code,
                hol_date= row.hol_date,
                hol_type= row.hol_type,
                hol_type_2= row.hol_type_2,
                states= row.states,
            )
            with db_engine.connect() as connection:
                connection.execute(insert_statement)
            #insert rows
            inserted_rows += 1
        except exc.IntegrityError as error:
            # print(error._message)
            duplicate_counter += 1
        except Exception as err:
            print(str(err))
            other_err_counter +=1
        
    print(f"{duplicate_counter + other_err_counter} errors encountered")
    print(f"{duplicate_counter} integrity/duplicate errors and {other_err_counter} other errors")
    print(f"{inserted_rows} inserted rows")


# run monthly
def run_etl():
    #countries = get_country_info(country_url)
    
    
    """delete from here"""
    #"""
    with open("/Users/kay/Documents/lab/calendarific/countries_sample.json") as countries_file:
        country_dict=json.load(countries_file)

    countries = [[country['iso-3166'], country['country_name']] for country in country_dict['response']['countries']]
    print(countries) #['response']['countries'])
    #"""
    """to here"""
    
    df_country = create_country_dataframe(countries)
    print(df_country.head())
    insert_countries(df_country)

    # holidays = get_holiday_for_all_countries(base_url)
    holidays = get_holidays_for_nigeria(base_url)
    dff_holiday = create_holiday_datafame(holidays)
    # print(dff_holiday)
    insert_holidays(dff_holiday)


# if __name__ == "__main__":
#     # bar will be invoked if this module is being run directly, but not via import!
#     etl()

# create offline copy
# df_holiday.to_csv('holidays.csv', index=False)
# df_holiday.reset_index().to_json('holidays.json', orient='records')
"""
for airflow

the countries tables must be created first because the holidays table references it (foreign key - country or country code)
the country index 

holiday table
holiday index

inserts
"""


run_etl()

# create function that takes the url and params and returns the payload
# create a function that prepares that data into lists or dictionaries
# create a function that creates a dataframe from the data
# create a function that loads dataframe to the database
# ensure data integrity ( no duplicates, correct values per column etc)
# handle errors

# send holidays a day before to your email.

# v2
# Get holidays for other countries
# send a summary of all the hoildays for the week to your email.