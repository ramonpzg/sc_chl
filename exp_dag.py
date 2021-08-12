from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


from random import randint

def choose_best(ti):
    accuracies = ti.xcom_pull(task_ids=[
        "training_model_1",
        "training_model_2",
        "training_model_3"
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accuracy'
    return 'inaccurate'

def training_mod():
    return randint(1, 10)


with DAG("my_exp", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:
    
    
    train_1 = PythonOperator(
        task_id="training_model_1",
        python_callable=training_mod
    
    )
    
    train_2 = PythonOperator(
        task_id="training_model_2",
        python_callable=training_mod
    
    )
    
    train_3 = PythonOperator(
        task_id="training_model_3",
        python_callable=training_mod
    
    )
    
    
    choose_best_mod = BranchPythonOperator(
        task_id="choose_best",
        python_callable=choose_best
    
    )
    
    
    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
        
    )
    
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
        
    )