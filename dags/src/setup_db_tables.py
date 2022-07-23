from sql_queries import sql_dict
from init_database import run_query, db_engine

# run_query(sql_dict['create_schema'])
# run_query(sql_dict['create_countries_table'])
# run_query(sql_dict['create_holidays_table'])
# run_query(sql_dict['create_holidays_index'])
# run_query(sql_dict['create_email_log_table']))

for sql, query in sql_dict.items():
    run_query(query)

