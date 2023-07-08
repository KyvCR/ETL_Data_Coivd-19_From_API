from Transform import *
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy import create_engine


# load raw data


def insert_raw_data():
    data = cleansing()
    data_df = pd.read_json(data, convert_dates=['Last Update'])
    table_name = 'data_covid'
    conn = engine = create_engine('mysql://root:354354@mysql:3306/data_engineer')
    data_df.to_sql(table_name, con=conn, if_exists='append', index=False)




