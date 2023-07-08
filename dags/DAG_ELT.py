from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


from Extract import *
from Transform import *
from Load import *

import requests as rs
import pandas as pd
import json


default_args = {
    'owner': 'Keyvi',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup': False,
    'email': ['keyvicaisarisman@gmail.com'],
    'email_on_failure': True, #jika failure akan mengirimkan email alert ke parameter email diatas
}

dag = DAG(
    dag_id='data_covid',
    default_args=default_args,
    schedule_interval='0 0 * * *' #akan berjalan setiap hari pada pukul 00:00 atau tengah malam
)



with dag:

    task1 = PythonOperator(
        task_id = 'Extract',
        python_callable=buat_list,
    )


    with TaskGroup(group_id='Load') as Load:
        task2 = PythonOperator(
            task_id = 'Cleansing',
            python_callable=cleansing,
        )

        task3 = PythonOperator(
            task_id = 'insert_data',
            python_callable=insert_raw_data,
        )
        task2 >> task3
        
    with TaskGroup(group_id='Trasnform') as Transform:
        #varuable didapat dari valriable yang ada dia riflow
        #connection mysqllocal mengambil dari connection yang ada di airflow


        create_table_country = MySqlOperator(
            task_id = 'create_table_country',
            mysql_conn_id='mysqllocal', 
            sql = Variable.get('query_create_table_country'), 
        )

        insert_table_country = MySqlOperator(
            task_id = 'insert_table_country',
            mysql_conn_id='mysqllocal',
            sql = Variable.get('query_insert_table_country'),
        )

        create_table_active_cases = MySqlOperator(
            task_id = 'create_table_active_cases',
            mysql_conn_id='mysqllocal',
            sql = Variable.get('query_create_table_active_case'),
        )

        insert_table_active_cases = MySqlOperator(
            task_id = 'insert_table_active_cases',
            mysql_conn_id='mysqllocal',
            sql = Variable.get('query_insert_table_active_cases'),
        )


        create_table_deaths = MySqlOperator(
            task_id = 'create_table_deaths',
            mysql_conn_id='mysqllocal',
            sql = Variable.get('query_create_table_deaths'),
        )

        insert_table_deaths = MySqlOperator(
            task_id = 'insert_table_deaths',
            mysql_conn_id='mysqllocal',
            sql = Variable.get('query_insert_table_deaths'),
        )


        create_table_active_cases >> insert_table_active_cases
        create_table_country >> insert_table_country
        create_table_deaths >> insert_table_deaths
       

task1 >> Load >> Transform
