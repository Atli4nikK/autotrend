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

from utils.autoru import autoru_parser


# --- КОНФИГУРАЦИЯ ПРОЕКТА ---
PROJECT_NAME = "autotrend"
DAG_ID = f"dag_{PROJECT_NAME}_autoru_regions"
DAG_DESCRIPTION = f"Параллельный сбор данных с auto.ru по регионам для проекта {PROJECT_NAME}"
DAG_TAGS = ["koldyrkaevs", PROJECT_NAME, "auto.ru", "parallel"]
DAG_SCHEDULE = None
DAG_CATCHUP = False

# Получаем путь к проекту
PROJECT_PATH = get_project_path(PROJECT_NAME)

# Определяем регионы для обработки
REGIONS = [
    'moskovskaya_oblast',                # Москва
    'leningradskaya_oblast',      # Санкт-Петербург
    'krasnodarskiy_kray',   # Краснодарский край
    'rostovskaya_oblast',               # Ростов
    'sverdlovskaya_oblast',         # Екатеринбург
    'novosibirskaya_oblast',          # Новосибирск
    'nizhegorodskaya_oblast',     # Нижний Новгород
    'tatarstan',                # Казань
    'chelyabinskaya_oblast',          # Челябинск
    'samarskaya_oblast',               # Самара
    'bashkortostan',                  # Уфа
    'volgogradskaya_oblast',            # Волгоград
    'permskiy_kray',                 # Пермь
    'krasnoyarskiy_kray',          # Красноярск
    'voronezhskaya_oblast',             # Воронеж
    'kurganskaya_oblast',             # Курган
    'kostromskaya_oblast',             # Кострома
    'kurskaya_oblast',             # Курск
    'belgorodskaya_oblast',             # Белгород
    'bryanskaya_oblast',             # Брянск
    'vladimirskaya_oblast',             # Владимир
    'vologodskaya_oblast',             # Вологда
    'dagestan',
    'chechenskaya_respublika',
    'kabardino-balkariya',
    'kaliningradskaya_oblast',
    'ivanovskaya_oblast',
    'kaluzhskaya_oblast',
    'lipetskaya_oblast',
    'orlovskaya_oblast',
    'ryazanskaya_oblast',
    'smolenskaya_oblast',
    'tverskaya_oblast',
    'tulskaya_oblast',
    'yaroslavskaya_oblast',
    'tambovskaya_oblast',
    'kareliya',
    'komi',
    'arhangelskaya_oblast',
    'murmanskaya_oblast',
    'novgorodskaya_oblast',
    'pskovskaya_oblast',
    'stavropolskiy_kray',
    'astrahanskaya_oblast',
    'mariy_el',
    'mordoviya',
    'udmurtiya',
    'chuvashskiya',
    'kirovskaya_oblast',
    'orenburgskaya_oblast',
    'penzenskaya_oblast',
    'saratovskaya_oblast',
    'ulyanovskaya_oblast',
    'tyumenskaya_oblast',
    'hanty-mansiyskiy_ao',
    'yamalo-nenetskiy_ao',
    'altayskiy_kray',
    'buryatiya',
    'tyva',
    'hakasiya',
    'irkutskaya_oblast',
    'kemerovskaya_oblast',
    'omskaya_oblast',
    'tomskaya_oblast',
    'chita',
    'saha_yakutiya',
    'primorskiy_kray',
    'habarovskiy_kray',
    'amurskaya_oblast',
    'kamchatskiy_kray',
    'magadanskaya_oblast',
    'sahalinskaya_oblast',
    'evreyskaya_ao',
    'ingushetiya',
    'severnaya_osetiya',
    'karachaevo-cherkesiya',
    'kalmykiya',
]

PRIORITY_REGIONS = ['moskovskaya_oblast', 'leningradskaya_oblast', 'krasnodarskiy_kray']
OTHER_REGIONS = [region for region in REGIONS if region not in PRIORITY_REGIONS]

# Создаем DAG
with DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(owner=PROJECT_NAME),
    description=DAG_DESCRIPTION,
    # schedule_interval=timedelta(days=7),
    schedule_interval=DAG_SCHEDULE,
    catchup=DAG_CATCHUP,
    tags=DAG_TAGS,
    concurrency=6,  # Увеличиваем количество параллельных задач
    max_active_runs=1  # Только один активный запуск DAG
) as dag:
    
    # ВАЖНО! Перед запуском DAG необходимо создать пул selenium_pool с 2 слотами в Airflow:
    # Admin -> Pools -> Create -> Name: selenium_pool, Slots: 2
    
    # Документация DAG
    dag.doc_md = """
    # Параллельный сбор данных с auto.ru по регионам
    
    Этот DAG запускает параллельные задачи для сбора данных о автомобилях с сайта auto.ru 
    для разных регионов России.
    
    ## Особенности
    * Каждый регион обрабатывается в отдельной задаче
    * Задачи выполняются параллельно (максимум 2 одновременно)
    * Приоритетные регионы (Москва, Санкт-Петербург и Краснодарский край) запускаются первыми
    * Остальные регионы имеют более низкий приоритет, но могут запускаться при наличии свободных слотов
    * Данные для каждого региона сохраняются в отдельные файлы
    * Используется Selenium Grid Hub для оптимальной работы
    
    ## Технические детали
    * Selenium Grid Hub с 2 максимальными сессиями
    * 1 Chrome нода с 2 сессиями
    * Параллельная обработка 2 регионов
    * Приоритизация задач через priority_weight
    * Автоматическая очередь для остальных регионов (если их больше 2)
    """
    
    # Создаем задачи для каждого региона
    region_tasks = {}
    for region in REGIONS:
        task_id = f"autoru_parser_{region}"
        
        region_tasks[region] = PythonOperator(
            task_id=task_id,
            python_callable=autoru_parser,
            op_kwargs={'region': region},
            pool='selenium_pool',  # Используем пул для контроля ресурсов
            pool_slots=1,  # Каждая задача занимает 1 слот
            retries=2,  # Добавляем повторные попытки
            retry_delay=timedelta(minutes=1)
        )
        
        # Добавляем описание к задаче
        region_tasks[region].doc_md = f"""
        ## Сбор данных с auto.ru для региона {region}
        
        Этот оператор запускает парсер для сбора данных с сайта auto.ru для региона {region}.
        
        ### Результаты сохраняются в файлы:
        - `/opt/airflow/dags/autotrend/data/{region}/json/downloaded_urls_autoru_*_{region}.json` - для хранения обработанных URL
        - `/opt/airflow/dags/autotrend/data/{region}/csv/car_data_*_{region}.csv` - для хранения собранных данных
        
        ### Параметры выполнения:
        - Повторные попытки: 2
        - Задержка между попытками: 1 минут
        """ 

    # Устанавливаем приоритеты для задач
    # Приоритетные регионы получат высокий вес, остальные - низкий
    # Это позволит Airflow сначала взять в работу приоритетные регионы,
    # но при наличии свободных слотов запустить и остальные регионы
    for region in REGIONS:
        if region in PRIORITY_REGIONS:
            region_tasks[region].priority_weight = 100  # Очень высокий приоритет
        else:
            region_tasks[region].priority_weight = 1   # Низкий приоритет 
            
    # Задача оповещения об успешном выполнении
    success = PythonOperator(
        task_id="success",
        python_callable=notify_on_success,
        doc_md="""
        ## Оповещение об успешном выполнении
        
        Отправляет уведомление об успешном выполнении DAG.
        """,
    )
    
    # Настраиваем зависимости: все задачи с регионами должны завершиться перед финальной задачей
    for region in REGIONS:
        region_tasks[region] >> success 