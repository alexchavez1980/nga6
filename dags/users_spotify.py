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


######################################################################################
#                FUNCIONES PARA API DE USUARIOS - SPOTIFY                            #
######################################################################################

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
    logging.info("OK")

######################################################################################
#                FUNCIONES PARA API DE PLAYLIST - SPOTIFY                            #
######################################################################################
def write_csv_pl(jp):
    csvheader = [
        'track_1',
        'track_2',
        'track_3',
        'track_4',
        'track_5',
        'track_6',
        'track_7',
        'track_8',
        'track_9',
        'track_10',
        'track_11',
        'track_12',
        'track_13',
        'track_14',
        'track_15',
        'track_16',
        'track_17',
        'track_18',
        'track_19',
        'track_20',
        'track_21',
        'track_22',
        'track_23',
        'track_24',
        'track_25',
        'track_26',
        'track_27',
        'track_28',
        'track_29',
        'track_30',
        'track_31',
        'track_32',
        'track_33',
        'track_34',
        'track_35',
        'track_36',
        'track_37',
        'track_38',
        'track_39',
        'track_40',
        'track_41',
        'track_42',
        'track_43',
        'track_44',
        'track_45',
        'track_46',
        'track_47',
        'track_48',
        'track_49',
        'track_50'
        ]
    df = pd.DataFrame(jp)
    df.to_csv('/opt/airflow/dags/csv/playlist_file.csv',index=False,header=csvheader)
    print("DONE")

def run_request_pl(url,querystring,headers):
    for x in querystring:
        response = requests.request("GET", url, headers=headers, params=x)
        myjson = response.json()
        j= myjson.get('items')
    return j

def api_playlist(playlists):
    url = "https://spotify23.p.rapidapi.com/playlist_tracks/"
    headers = {
       # "X-RapidAPI-Key": "6cfaaf4829msh6164b68312c045cp187fcejsn97b5941093c9",
        "X-RapidAPI-Key": "c4d262eb60msh8ccfaa2c82311efp16ba53jsn09d3a0ac81f9",
        "X-RapidAPI-Host": "spotify23.p.rapidapi.com"
    }
    pl = []
    for i in playlists:
        querystring = [
            {"id":i,"offset":"0","limit":"50"}
        ]
    
        json_playlist = run_request_pl(url,querystring,headers)
        print(querystring)
        print("""
        
        OK - RESULTADO DE LA API
        
        """)
        print(json_playlist)
        pl.append(json_playlist)

        
    return pl

def get_playlists_db():
    conn_string = "host='host.docker.internal'  port='5433' dbname='SEMINARIO' user='postgres' password='1234'"
   # print("Connecting to database\n	->%s".format(conn_string))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("select playlist_id from playlists ")
    a_playlist = []
    records = cursor.fetchall()
    for x in records:
        a_playlist.append(x[0])
        print(x[0])

    return a_playlist

def run_playlist():
    playlists = get_playlists_db()
    print("""
    
    voy a printar las playlist en el MAIN
    
    """)
    print(playlists)
    jp = api_playlist(playlists)
    write_csv_pl(jp)

######################################################################################
#                       CONFIGURACIÓN DEL DAG - AIRFLOW                              #
######################################################################################


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

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgresl_local",
        sql="sql/create_tables.sql",
    )
    api_extract_users = PythonOperator(
        task_id='api_extract_users',
        python_callable=run
    )
    insert_staging_users_file = PostgresOperator(
        task_id="insert_staging_users_file",
        postgres_conn_id="postgresl_local",
        sql="sql/insert_table_stage_users_file.sql",
    )
    insert_users = PostgresOperator(
        task_id="insert_users",
        postgres_conn_id="postgresl_local",
        sql="sql/insert_table_user_final.sql",
    )
    insert_playlists_users = PostgresOperator(
        task_id="insert_playlists_users",
        postgres_conn_id="postgresl_local",
        sql="sql/insert_playlist_users.sql",
    )
    api_extract_playlist = PythonOperator(
        task_id='api_extract_playlist',
        python_callable=run_playlist
    )
    insert_staging_playlist_file = PostgresOperator(
        task_id="insert_staging_playlist_file",
        postgres_conn_id="postgresl_local",
        sql="sql/insert_table_stage_playlist_file.sql",
    )
    insert_playlist_artist = PostgresOperator(
        task_id="insert_playlist_artist",
        postgres_conn_id="postgresl_local",
        sql="sql/insert_playlist_artist.sql",
    )
    insert_traspuesta_artista_userid = PostgresOperator(
        task_id="insert_traspuesta_artista_userid",
        postgres_conn_id="postgresl_local",
        sql="sql/insert_traspuesta_artista_userid.sql",
    )

    create_tables >> api_extract_users >> insert_staging_users_file >> insert_users >> insert_playlists_users >> api_extract_playlist >> insert_staging_playlist_file >> insert_playlist_artist >> insert_traspuesta_artista_userid