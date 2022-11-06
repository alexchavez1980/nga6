from asyncio import tasks
from datetime import timedelta
import json

import requests
import pandas as pd
import psycopg2
import sys

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task 
from airflow import DAG 

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine
import logging


##funciones

def extract_data ():
        url = "https://spotify23.p.rapidapi.com/user_profile/"
        a_query = users_generator()
        headers = {
            "X-RapidAPI-Key": "c4d262eb60msh8ccfaa2c82311efp16ba53jsn09d3a0ac81f9",
            "X-RapidAPI-Host": "spotify23.p.rapidapi.com"
        }
        json_users = []
        json_users = run_request(url,a_query,headers)
        print(json_users)
        write_csv(json_users)
        return json_users

def users_generator():
    #Manualmente seteo en una lista los id's de los usuarios a consultar en la api.
    querystring = [
        {"id":"21gt55n63swkgukdfd5tvfyni","playlistLimit":"10","artistLimit":"10"},
        {"id":"11178273764","playlistLimit":"10","artistLimit":"10"},
        {"id":"11162160084","playlistLimit":"10","artistLimit":"10"}
    ]

        #print(querystring)
    return querystring

def run_request(url,a_query,headers):
    a_myjson =  []
    for x in a_query:
        response = requests.request("GET", url, headers=headers, params=x)
        myjson = response.json()
            
        a_myjson.append(myjson)
           
    return a_myjson 

def write_csv(json_users):
    csvheader = [
        'uri',
        'name',
        'image_url',
        'followers_count',
        'following_count',
        'public_playlists',
        'total_public_playlists_count',
        'has_spotify_name',
        'has_spotify_image',
        'color',
        'user_created_show'
        ]
    df = pd.DataFrame(json_users)
    df.to_csv('/opt/airflow/dags/csv/users_file.csv',index=False,header=csvheader)
    print("DONE")

def run():
    df = extract_data()
    print('a ver si anduvo voy a listar ')
    print(df)
    logging.info("LA RE CONCHA DE TU MADRE ALLBOYS")

###comienza config del dag


default_args = {
    'owner':'seminario-itba',
    'depends_on_past':False,
    'email':['nicolasarosteguy@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'users_spotify',
    default_args=default_args,
    description='segunda prueba',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['le pega a la api de spotify y trae playlist de usuarios'],
) as dag:

    create_table_users = PostgresOperator(
        task_id="create_table_users",
        postgres_conn_id="postgres_nga_test",
        sql="sql/create_table_users.sql",
    )
    extract = PythonOperator(
        task_id='extract',
        python_callable=run
    )
    insert_table_users_file = PostgresOperator(
        task_id="insert_table_users_file",
        postgres_conn_id="postgres_nga_test",
        sql="sql/insert_table_users_file.sql",
    )

    create_table_users >> extract >> insert_table_users_file