#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
		'owner': 'Ranga',
		'start_date': datetime(2022, 3, 4),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
hello_world_dag = DAG('hello_world_NGA_dag',
		default_args=default_args,
		description='Hello World NGA DAG',
		schedule_interval='* * * * *', 
		catchup=False,
		tags=['example, helloworld']
)

# python callable function
def print_hello():
		return 'Hello World NGA!'

# Creating first task
start_task = DummyOperator(task_id='start_task', dag=hello_world_dag)

# Creating second task
hello_world_task = PythonOperator(task_id='hello_world_task', python_callable=print_hello, dag=hello_world_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=hello_world_dag)

# Set the order of execution of tasks. 
start_task >> hello_world_task >> end_task
# --------------------------------------------------------------------------
# Pruebas inconexas. Abajo hago el mismo test de siempre:
# --------------------------------------------------------------------------
#pip install yfinance
#import pandas as pd
#import numpy as np
#import yfinance as yf
#import datetime
#pd.options.display.float_format = '{:.4f}'.format # Flotantes de 4 decimales.

#ds_BTC = yf.download('BTC-USD', period = '1mo', interval = '60m')
#ds_BTC.describe()

#ds_BTC['DateTime'] = ds_BTC.index # Importante: Incluyo la cronología en una columna
#ds_BTC = ds_BTC[['DateTime','Open','High','Low','Close','Adj Close','Volume']]
#ds_BTC.head()

#ds_BTC.to_csv(r'./ds_BTC_PERSISTENCIA_AIRFLOW.csv') # , sep='\t', encoding='utf-8', header='true'
