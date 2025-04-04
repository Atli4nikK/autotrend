import os
import sys
from datetime import datetime, timedelta

# Добавляем путь к общим утилитам и к директории dags
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/dags/autotrend')
from common.utils import get_default_args, get_project_path, get_project_config
from common.notify import notify_on_failure, notify_on_success

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from utils.load_stg_autoru_offers import load_csv_to_stg

# Параметры по умолчанию для DAG

# Параметры DAG
PROJECT_NAME = "autotrend"
DAG_ID = f"dag_{PROJECT_NAME}_load_stg_autoru_offers"
DAG_DESCRIPTION = "Загрузка данных из CSV в STG слой"
DAG_SCHEDULE = None
DAG_TAGS = ["koldyrkaevs", PROJECT_NAME, "auto.ru", "stg"]

# Создание DAG
with DAG(
    DAG_ID,
    default_args=get_default_args(owner=PROJECT_NAME),
    description=DAG_DESCRIPTION,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=DAG_TAGS
) as dag:

    # Документация DAG
    dag.doc_md = """
    # Загрузка данных из CSV в STG слой
    
    Этот DAG загружает данные из CSV файлов в таблицу STG слоя.
    
    ## Особенности
    * Чтение файлов чанками для оптимизации памяти
    * Загрузка данных в stg_autotrend.autoru_offers
    * Добавление технических полей: load_date и run_id
    
    ## Технические детали
    * Размер чанка: 10,000 строк
    * Коннекшн: dwh_prod_conn
    """

    # Создание задачи
    load_stg_task = PythonOperator(
        task_id="load_csv_to_stg",
        python_callable=load_csv_to_stg
    )

    # Задача оповещения об успешном выполнении
    success = PythonOperator(
        task_id="success",
        python_callable=notify_on_success,
        doc_md="""
        ## Оповещение об успешном выполнении
        
        Отправляет уведомление об успешном выполнении DAG.
        """,
    )

    # Определение порядка выполнения задач
    load_stg_task >> success
