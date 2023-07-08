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
    'catchup': False
}

dag = DAG(
    dag_id='data_covid',
    default_args=default_args,
    schedule_interval='0 0 * * *'
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
        # task3 = PythonOperator(
        #     task_id = 'Createtable_Country',
        #     python_callable=createtable_Country,
        # )
        # task4 = PythonOperator(
        #     task_id = 'createtable_ActiveCasesData',
        #     python_callable=createtable_ActiveCasesData,
        # )
        # task5 = PythonOperator(
        #     task_id = 'createtable_NewCaseData',
        #     python_callable=createtable_NewCaseData,
        # )
        # task6 = PythonOperator(
        #     task_id = 'createtable_DeathsData',
        #     python_callable=createtable_DeathsData,
        # )

        task2 >> task3
        
        #[task3, task4, task5, task6]

    with TaskGroup(group_id='Trasnform') as Transform:

        #query_create_table_country = Variable.get('query_create_table_country')
        create_table_country = MySqlOperator(
            task_id = 'create_table_country',
            mysql_conn_id='mysqllocal',
            sql = Variable.get('query_create_table_country'),
        )

        #query_insert_table_country = Variable.get('query_insert_table_country')
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
       

    # with TaskGroup(group_id='Load') as Load:
    #     # task7 = PythonOperator(
    #     #     task_id = 'insert_data',
    #     #     python_callable=insert_data(createtable_ActiveCasesData(),'active_case_test'),
    #     # )    

    #     task8 = PythonOperator(
    #         task_id = 'insert_data_country',
    #         python_callable=insert_data_country(),
    #     )    

        # task9 = PythonOperator(
        #     task_id = 'insert_data_active_case',
        #     python_callable=insert_data_active_case(),
        # )   

        # task10 = PythonOperator(
        #     task_id = 'insert_data_NewCaseData',
        #     python_callable=insert_data_NewCaseData(),
        # )

        # task11 = PythonOperator(
        #     task_id = 'insert_data_DeathsData',
        #     python_callable=insert_data_DeathsData(),
        # )

        
    #with TaskGroup(group_id="group1") as tg1:

    # task3 = PythonOperator(
    #         task_id = 'clenasingdata',
    #         python_callable=cleansing_data,
    # )

    # task4 = PythonOperator(
    #         task_id = 'test',
    #         python_callable=createTable_Country,
    # )

    # task4 = PythonOperator(
    #         task_id = 'test',
    #         python_callable=insert_data,
    #         op_arg=['createtable_Country()', 'Country']
    # )


   #task3  >> task4

task1 >> Load >> Transform