from datetime import timedelta
from textwrap import dedent
import pandas as pd, os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator


message_http = "3rd_party_data.json"
message_bus = "message_bus.json"
databases = "database.csv"
path_4_transformed_data = "data"
name_4_transformed_data = "cleaned_data.parquet"



dag = DAG(dag_id='etl_safety', 
          description='A simple tutorial DAG',
          start_date=days_ago(2),
          schedule_interval=None
         )

def read_message_http(file):
    return pd.read_json(file, lines=True)


etract_1 = PythonOperator(
         task_id='read_http_messages',
         python_callable=read_message_http,
         op_kwargs={"file": message_http},
         dag=dag
)

def read_message_bus(file):
    data_list = [] # empty list that will hold a line of data for us

    for line in open(file, 'r'):
        try:
            data_list.append(json.loads(line))
        except:
            continue
    
    return pd.DataFrame(data_list)


etract_2 = PythonOperator(
         task_id='read_bus_messages',
#          depends_on_past=False,
         python_callable=read_message_bus,
         op_kwargs={"file": message_bus},
         dag=dag
)

def read_from_database(file):
    return pd.read_csv(file)

etract_3 = PythonOperator(
         task_id='read_databases',
#          depends_on_past=False,
         python_callable=read_from_database,
         op_kwargs={"file": databases},
         dag=dag
)


def transform_merge(etract_1, etract_2, etract_3):
    
    extract_2 = extract_2.drop('media', axis=1).explode("items", ignore_index=True)
    
    return (etract_1.merge(extract_2, left_on='user_id', right_on='user_id')
                    .merge(extract_3, left_on='email', right_on='user_email'))

transform = PythonOperator(
                task_id='transform_and_merge',
                python_callable=transform_merge,
                op_kwargs={"etract_1": etract_1, 
                           'etract_2': etract_2,
                           'etract_3': etract_3},
                dag=dag
)


def load_data(path, name, transformed_data):
    transformed_data.to_parquet(os.path.join(path, name))

load = PythonOperator(
    task_id='load_the_data',
    python_callable=load_data,
    op_kwargs={"path": path_4_transformed_data, 
               'name': name_4_transformed_data,
               'transformed_data': transform},
    dag=dag
)



[etract_1, etract_2, etract_3] >> transform >> load