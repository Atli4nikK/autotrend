# AutoTrend

Анализ рынков автомобилей

## Описание

Проект AutoTrend предназначен для сбора, обработки и анализа данных об автомобилях с различных источников. Проект использует Apache Airflow для оркестрации ETL-процессов и включает в себя различные DAG-и для обработки данных.

## Структура проекта

```
autotrend/
├── airflow/ # Директория с файлами Airflow
│   ├── dags/ # DAG-файлы Airflow для оркестрации задач
│   │   ├── autotrend_raw_data_layer.py # DAG для обработки сырых данных
│   │   ├── dag_autotrend_autoru_regions.py # DAG для обработки регионов Auto.ru
│   │   ├── dag_autotrend_example.py # Пример DAG-файла
│   │   ├── utils/ # Утилиты для DAG-ов
│   │   ├── data/ # Вспомогательные данные
│   │   └── __init__.py # Инициализация пакета
├── db/ # Директория с файлами БД
│   ├── stg_autotrend/ # Схема STG (Staging)
│   │   ├── tables/ # Таблицы
│   │   ├── views/ # Представления
│   │   ├── functions/ # Функции
│   │   ├── grants/ # Гранты
│   │   ├── scripts/ # Вспомогательные скрипты
│   │   └── schema.sql # Создание схемы
│   ├── ods_autotrend/ # Схема ODS (Operational Data Store)
│   │   ├── tables/
│   │   ├── views/
│   │   ├── functions/
│   │   ├── grants/
│   │   ├── scripts/
│   │   └── schema.sql
│   ├── dds_autotrend/ # Схема DDS (Detail Data Store)
│   │   ├── tables/
│   │   ├── views/
│   │   ├── functions/
│   │   ├── grants/
│   │   ├── scripts/
│   │   └── schema.sql
│   ├── dm_autotrend/ # Схема DM (Data Marts)
│   │   ├── tables/
│   │   ├── views/
│   │   ├── functions/
│   │   ├── grants/
│   │   ├── scripts/
│   │   └── schema.sql
│   └── README.md # Документация по БД
├── diagram.vuerd.json # ER-диаграмма базы данных
├── .gitignore # Исключения для Git
└── README.md # Документация проекта
```

## Установка и настройка

Данный репозиторий предназначен для использования с инстансом Airflow, где он монтируется как директория с DAG-файлами.

1. Клонировать репозиторий:
   ```bash
   git clone https://github.com/Atli4nikK/autotrend.git
   ```

2. Обеспечить подключение репозитория к Airflow через docker-compose.yaml:
   ```yaml
   volumes:
     - /path/to/autotrend/dags:/opt/airflow/dags/autotrend
   ```

3. Инициализировать базу данных:
   ```bash
   # Создание схем и базовых объектов
   psql -f db/stg_autotrend/schema.sql
   psql -f db/ods_autotrend/schema.sql
   psql -f db/dds_autotrend/schema.sql
   psql -f db/dm_autotrend/schema.sql
   
   # Назначение прав доступа
   psql -f db/stg_autotrend/grants/01_basic_grants.sql
   psql -f db/ods_autotrend/grants/01_basic_grants.sql
   psql -f db/dds_autotrend/grants/01_basic_grants.sql
   psql -f db/dm_autotrend/grants/01_basic_grants.sql
   ```

## Использование

После подключения репозитория к Airflow, DAG-и будут доступны в веб-интерфейсе Airflow с префиксом "autotrend_". Основные DAG-и:

- `autotrend_raw_data_layer`: Обработка сырых данных
- `autotrend_autoru_regions`: Обработка регионов Auto.ru

## Требования

- Apache Airflow 2.0+
- Python 3.8+
- PostgreSQL/Greenplum
- Доступ к общим библиотекам, определенным в директории common

## Разработка

При разработке новых DAG-ов для AutoTrend следуйте этим рекомендациям:

1. Используйте префикс "autotrend_" для имен DAG-ов
2. Добавляйте тег "autotrend" для всех DAG-ов
3. Используйте общие утилиты из директории common через правильный импорт:
   ```python
   import sys
   sys.path.append('/opt/airflow')
   from common.utils import get_default_args
   ```
4. Следуйте структуре проекта и размещайте новые DAG-и в директории `airflow/dags/`
5. При необходимости добавляйте новые утилиты в директорию `airflow/dags/utils/`
6. При создании новых объектов БД следуйте структуре директорий в папке `db/`
7. Документируйте все изменения в схеме базы данных
