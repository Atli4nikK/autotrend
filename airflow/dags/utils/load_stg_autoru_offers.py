def load_csv_to_stg(**context):
    """
    Загружает данные из CSV файлов в STG слой.
    Сначала объединяет все файлы в один DataFrame, затем загружает чанками.
    После успешной загрузки сохраняет объединенный файл и удаляет исходные.
    
    Примеры получения данных из объединенного файла:
    1. Получить данные за определенный период:
       df = pd.read_csv('autoru_offers.csv')
       df['load_date'] = pd.to_datetime(df['load_date'])
       period_data = df[
           (df['load_date'] >= '2024-03-15 00:00:00') & 
           (df['load_date'] <= '2024-03-15 23:59:59')
       ]
       
    2. Получить данные определенного запуска:
       df = pd.read_csv('autoru_offers.csv')
       run_data = df[df['load_id'] == 'specific_run_id']
       
    3. Получить последние данные для каждого URL:
       df = pd.read_csv('autoru_offers.csv')
       df['load_date'] = pd.to_datetime(df['load_date'])
       latest_data = df.sort_values('load_date').drop_duplicates(
           subset=['url', 'price'], 
           keep='last'
       )
    """
    try:
        # Импорты внутри функции для оптимизации загрузки модулей
        from datetime import datetime
        import os
        import glob
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # Путь к директории с данными /opt/airflow/autotrend/data
        data_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'autotrend/data/regions')
        
        # Получаем список всех CSV файлов
        csv_files = glob.glob(os.path.join(data_dir, '**/*.csv'), recursive=True)
        if not csv_files:
            print("CSV файлы не найдены")
            return
            
        print(f"Найдено файлов: {len(csv_files)}")
        
        # Читаем заголовки из первого файла
        headers = pd.read_csv(csv_files[0]).columns.tolist()
        
        # Читаем все CSV файлы в список DataFrame'ов
        dfs = []
        # Первый файл читаем с заголовками
        dfs.append(pd.read_csv(csv_files[0]))
        
        # Остальные файлы читаем без заголовков
        for file in csv_files[1:]:
            df = pd.read_csv(file, names=headers, skiprows=1)
            dfs.append(df)
            
        # Объединяем все DataFrame'ы
        combined_df = pd.concat(dfs, ignore_index=True)
        total_rows = len(combined_df)
        print(f"Всего строк в объединенном DataFrame: {total_rows}")
        
        # Добавляем технические поля
        current_time = datetime.now()
        combined_df['load_date'] = current_time  # Обновляем существующее поле load_date
        combined_df['load_id'] = context['dag_run'].run_id
        
        # Подключаемся к PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='dwh_prod_conn')
        
        # Загружаем данные через to_sql
        engine = pg_hook.get_sqlalchemy_engine()
        combined_df.to_sql(
            name='autoru_offers',
            schema='stg_autotrend',
            con=engine,
            if_exists='append',
            index=False
        )
                
        print(f"Загрузка завершена. Всего загружено строк: {total_rows}")
        
        # Путь к истории
        history_dir = os.path.join(data_dir, '..', 'history')
        if not os.path.exists(history_dir):
            os.makedirs(history_dir)
            
        history_file = os.path.join(history_dir, 'autoru_offers.csv')
        
        # Добавляем новые данные в конец файла
        if os.path.exists(history_file):
            # Добавляем без заголовков
            combined_df.to_csv(history_file, mode='a', header=False, index=False)
        else:
            # Создаем новый файл с заголовками
            combined_df.to_csv(history_file, mode='w', index=False)
            
        print(f"История обновлена: {history_file}")
        print(f"Добавлено новых записей: {total_rows}")
        
        # Удаляем исходные CSV файлы
        for file in csv_files:
            try:
                os.remove(file)
                print(f"Удален исходный файл: {file}")
            except Exception as e:
                print(f"Ошибка при удалении файла {file}: {str(e)}")
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {str(e)}")
        raise 