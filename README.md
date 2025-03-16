# AutoTrend

Анализ рынков автомобилей

## Описание

## Структура проекта

```
autotrend/
├── airflow/ # Директория с файлами Airflow
│ ├── dags/ # DAG-файлы Airflow для оркестрации задач
│ │ ├── autotrend_example.py # Пример DAG-файла
│ │ └── init.py # Инициализация пакета
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

## Использование

После подключения репозитория к Airflow, DAG-и будут доступны в веб-интерфейсе Airflow с префиксом "autotrend_".

## Требования

- Apache Airflow 2.0+
- Python 3.8+
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
