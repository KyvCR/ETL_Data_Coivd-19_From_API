from Transform import *
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy import create_engine


# def conn_db():
#     host = "localhost"
#     user = "root"
#     password = '354354'
#     database = "data_engineer"
#     port = 3306

#     engine = create_engine(f'mysql://{user}:{password}@{host}:{port}/{database}')
#     return engine
# ''' coba membuat satu function nanti data dan nama tabel di isi'''
def insert_data(data, table):
    data = data
    conn = conn_db()
    table = table
    data.to_sql(table, con=conn, if_exists='append', index=False)


def data():
    df = pd.read_json(cleansing())
    df_Active_Cases = df[(df["Active Cases_text"]!=0) & (df['Active Cases_text'] != 'N/A')].reset_index()
    df_Active_Cases = df_Active_Cases[['Country_ID', 'Active Cases_text','Total Cases_text','Total Recovered_text', 'Last Update']]
   
    return df_Active_Cases


def insert_data_country():
    data = createtable_Country()
    conn = conn_db()
    table_name= 'country'
    data.to_sql(table_name, con=conn, if_exists='append', index=False)

def insert_data_active_case():
    data = createtable_ActiveCasesData()
    conn = conn_db()
    table_name= 'active_case'
    data.to_sql(table_name, con=conn, if_exists='append', index=False)

# def insert_data_NewCaseData():
#     data = createtable_NewCaseData()
#     conn = conn_db()
#     table_name= 'New_Case_Data'
#     data.to_sql(table_name, con=conn, if_exists='append', index=False)

# def insert_data_DeathsData():
#     data = createtable_DeathsData()
#     conn = conn_db()
#     table_name= 'Deaths_Data'
#     data.to_sql(table_name, con=conn, if_exists='append', index=False)   


# query_create_country = '''
#     CREATE TABLE country ( 
#     country_id int primary key,
#     country_name varchar(50),
#     )
#     '''

# query_insert_table_country = '''
#     insert into country values (%s, %s)
#     '''

# query_create_active_case_data = '''
#     CREATE TABLE active_case(
#     country_id int primary key,
#     active_case int,
#     total_case int,
#     total_recovered int,
#     last_update datetime
#     )
#     '''

# query_insert_table_active_case_data = '''
#     insert into active_case values (%s, %s, %s, %s, %s)
#     '''

# query_create_new_case_data = '''
#     CREATE TABLE new_case(
#     country_id int primary key,
#     new_case int,
#     last_update datetime
#     )
#     '''

# query_insert_table_new_case = '''
#     insert into new_case values (%s, %s, %s)
#     '''

# query_create_deaths_data ='''
#     CREATE TABLE deaths_case(
#     country_id int primary key,
#     new_deaths int,
#     total_deaths int,
#     last_update datetime
#     )
#     '''

# query_insert_table_deaths_case = '''
#     insert into deaths_case values (%s, %s, %s, %s)
#     '''


# def create_table(table, table_name ,query):
#     df = table
#     table_name = table_name
#     mysql_conn = MySqlHook(mysql_conn_id='data_engineer')
#     conn = mysql_conn.get_conn()
#     cursor = conn.cursor()
#     cursor.execute(query)
#     cursor.close()
#     cursor.commit()

#     df.to_sql(table_name, mysql_conn.get_sqlalchemy_engine(), index=False)
