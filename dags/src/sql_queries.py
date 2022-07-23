
create_schema = ("create schema if not exists calendarific authorization calendarific;")

create_countries_table= """
create table if not exists calendarific.countries (
    country_code varchar(4) primary key,
    country varchar(30)
);
"""

create_holidays_table= """
create table if not exists calendarific.holidays (
    holiday varchar(100),
    description varchar(200),
    country_code varchar(4) references calendarific.countries(country_code),
    hol_date date,
    year integer,
    hol_type varchar(200),
    hol_type_2 varchar(200),
    states varchar(20)
);
"""

create_holidays_index="""
CREATE UNIQUE INDEX IF NOT EXISTS holidays_pkey ON calendarific.holidays USING btree (country_code, hol_date);
"""

create_countries_index="""
CREATE UNIQUE INDEX countries_pkey ON calendarific.countries USING btree (country_code);
"""

create_email_log_table="""
CREATE TABLE if not exists calendarific.log_email (
	sent_to varchar(40) NULL,
	date_sent date NULL,
	time_sent varchar(8) NULL
);
"""

sql_dict = { 
    'create_schema': create_schema,
    'create_countries_table': create_countries_table,
    'create_holidays_table': create_holidays_table,
    'create_holidays_index': create_holidays_index,
    'create_countries_index': create_countries_index,
    'create_email_log_table': create_email_log_table
}