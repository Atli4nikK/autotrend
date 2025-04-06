def autoru_parser(region=None, **kwargs):
    """
    Парсер для сбора данных об автомобилях с сайта auto.ru
    
    Args:
        region (str): Регион для парсинга (например, 'moscow', 'spb')
        **kwargs: Дополнительные параметры
    
    Returns:
        None: Результаты сохраняются в CSV-файлы в соответствующих директориях
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    import time
    import os
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
    import pandas as pd
    from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
    from datetime import datetime
    import json


    # Настройка директорий для хранения данных
    SAVE_FOLDER = f"/opt/airflow/autotrend/data/regions/{region}/json"  # Директория для JSON-файлов
    SAVE_FOLDER_CSV = f"/opt/airflow/autotrend/data/regions/{region}/csv"  # Директория для CSV-файлов
    
    print(f"Запуск парсера для региона: {region}")

    def load_downloaded_urls(car_mark: str) -> dict:
        """
        Загружает словарь ранее обработанных URL и их цен из JSON-файла
        
        Args:
            car_mark (str): Марка автомобиля
            
        Returns:
            dict: Словарь вида {URL: цена} или пустой словарь, если файл не существует
        """
        file_path=f'/opt/airflow/autotrend/data/regions/{region}/json/downloaded_urls_autoru_{car_mark}_{region}.json'
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as file:
                return json.load(file)
        return {}

    def save_downloaded_urls(car_mark: str, urls: dict) -> None:
        """
        Сохраняет словарь обработанных URL и их цен в JSON-файл
        
        Args:
            car_mark (str): Марка автомобиля
            urls (dict): Словарь вида {URL: цена}
            
        Returns:
            None
        """
        file_path=f'/opt/airflow/autotrend/data/regions/{region}/json/downloaded_urls_autoru_{car_mark}_{region}.json'
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(urls, file, ensure_ascii=False, indent=4)
    
    def setup_chrome_optimizations(driver):
        """
        Применяет оптимизации для браузера Chrome для ускорения парсинга
        
        Args:
            driver: Экземпляр WebDriver
            
        Returns:
            None
        """
        # Отключение кэширования и блокировка ненужных ресурсов
        driver.execute_cdp_cmd('Network.setCacheDisabled', {'cacheDisabled': False})
        driver.execute_cdp_cmd('Network.setBlockedURLs', {"urls": ["*.jpg", "*.jpeg", "*.png", "*.gif", "*.css", "*.woff", "*.woff2", "*.ttf", "*.js"]})
        driver.execute_cdp_cmd('Page.setDownloadBehavior', {'behavior': 'deny'})
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/96.0.4664.45'})
        
        # Ограничение времени загрузки страницы
        driver.set_page_load_timeout(5)
        driver.set_script_timeout(5)

    # Создание директорий для хранения данных, если они не существуют
    if not os.path.exists(SAVE_FOLDER):
        os.makedirs(SAVE_FOLDER)
        print(f"Создана директория для JSON-файлов: '{SAVE_FOLDER}'")
    else:
        print(f"Директория для JSON-файлов уже существует: '{SAVE_FOLDER}'")

    if not os.path.exists(SAVE_FOLDER_CSV):
        os.makedirs(SAVE_FOLDER_CSV)
        print(f"Создана директория для CSV-файлов: '{SAVE_FOLDER_CSV}'")
    else:
        print(f"Директория для CSV-файлов уже существует: '{SAVE_FOLDER_CSV}'")

    # Настройка опций браузера Chrome
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  # Запуск в фоновом режиме
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')  # Отключение загрузки изображений
    chrome_options.add_argument("--disable-dev-shm-usage")  # Решение проблем с памятью
    chrome_options.add_argument('--no-sandbox')  # Отключение изоляции для Docker
    chrome_options.add_argument('--ignore-certificate-errors')  # Игнорирование ошибок сертификатов
    chrome_options.add_argument('--ignore-ssl-errors')  # Игнорирование ошибок SSL

    # Дополнительные оптимизации для повышения производительности
    chrome_options.add_argument('--disable-extensions')  # Отключение расширений
    chrome_options.add_argument('--disable-browser-side-navigation')  # Отключение навигации браузера
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')  # Отключение композитора
    chrome_options.add_argument('--disable-infobars')  # Отключение информационных панелей
    chrome_options.page_load_strategy = 'eager'  # Загрузка только необходимого контента

    # Инициализация WebDriver с использованием Selenium Grid
    driver = webdriver.Remote(
        command_executor='http://selenium-chrome:4444',
        options=chrome_options
    )
    print(f"Браузер запущен для региона {region} через Selenium Chrome Standalone")
    
    # Применение оптимизаций для браузера
    setup_chrome_optimizations(driver)
    
    # Параметры ожидания и счетчики
    wait_time = 0.2  # Минимальное время ожидания между запросами (сек)
    total_cars = 0  # Общее количество обработанных объявлений
    
    # Фиксация времени начала выполнения для статистики
    start_time = datetime.now()
    try:
        print(f"Начало обработки региона: '{region}'")
        
        # Открытие начальной страницы и нажатие на кнопку "Все марки"
        try:
            driver.get(f"https://auto.ru/{region}/")
            time.sleep(wait_time)
            show_more_button = driver.find_element(By.CSS_SELECTOR, ".IndexMarks__show-all")   
            if show_more_button:
                show_more_button.click()
                print("Кнопка 'Все марки' успешно нажата")
        except Exception as e:
            print(f"!!!Кнопка 'Все марки' не найдена!!!")

        # Сбор списка всех марок автомобилей
        # Ищем только марки с объявлениями (класс IndexMarks__marks-with-counts)
        elements = driver.find_elements(By.CLASS_NAME, "IndexMarks__item")
        
        car_marks_set = set()  # Используем множество для исключения дублей

        for element in elements:
            # Извлечение марки из URL-адреса
            parts = element.get_attribute("href").split('/')
            index = parts.index('cars')
            car_mark = parts[index + 1]  # Часть URL после 'cars'
            car_marks_set.add(car_mark)
        
        car_marks = list(car_marks_set)  # Преобразуем обратно в список для дальнейшей обработки
        print(f"Найдено уникальных марок автомобилей: {len(car_marks)}")
        print(f"Список марок: {car_marks}")

        # Обработка каждой марки автомобиля
        for car_mark in car_marks:
            print(f"=== Начало обработки марки: '{car_mark}' ===")
            
            # Загрузка ранее обработанных URL для отслеживания изменений
            downloaded_urls = load_downloaded_urls(car_mark)
            print(f"Загружено {len(downloaded_urls)} ранее обработанных URL для марки '{car_mark}'")
            
            # Открытие страницы марки и нажатие на кнопку "Все модели"
            try:
                driver.get(f"https://auto.ru/{region}/cars/{car_mark}/all/")
                time.sleep(wait_time)
                show_more_button = driver.find_element(By.CSS_SELECTOR, ".ListingPopularMMM__expandLink")
                if show_more_button:
                    show_more_button.click()
                    print("Кнопка 'Все модели' успешно нажата")
            except Exception as e:
                print(f"!!!Кнопка 'Все модели' не найдена!!!")

            # Сбор списка всех моделей для текущей марки
            elements = driver.find_elements(By.CLASS_NAME, "ListingPopularMMM__itemName")
            car_models = []

            for element in elements:
                # Извлечение модели из URL-адреса
                parts = element.get_attribute("href").split('/')
                index = parts.index(car_mark)
                car_model = parts[index + 1]  # Часть URL после марки
                car_models.append(car_model)
            
            print(f"Найдено моделей для марки '{car_mark}': {len(car_models)}")
            print(f"Список моделей: {car_models}")

            all_data = []  # Список для хранения всех данных объявлений
            all_urls = {}  # Словарь для хранения URL всех моделей
            
            # Первый этап: сбор URL-адресов объявлений для всех моделей
            for car_model in car_models:
                print(f"Сбор URL для модели '{car_model}' марки '{car_mark}'...")
                urls = set()  # Используем множество для исключения дублей
                page = 1
                
                # Постраничный обход каталога объявлений
                while True:

                    driver.get(f"https://auto.ru/{region}/cars/{car_mark}/{car_model}/all/?output_type=table&page={page}")
                    time.sleep(wait_time)
                    
                    # Получение всех объявлений на текущей странице
                    listings = driver.find_elements(By.CSS_SELECTOR, ".ListingItemSequential__enclose")

                    # Если объявлений нет - достигли последней страницы
                    if len(listings) == 0:
                        break
                        
                    # Обработка каждого объявления на странице
                    for listing in listings:
                        try:
                            # Извлечение ссылки и цены из объявления
                            car_link = listing.find_element(By.CSS_SELECTOR, ".ListingItemTitle__link").get_attribute("href")
                            car_price = listing.find_element(By.CSS_SELECTOR, ".ListingItemPrice__content, .ListingItemPriceNew__content-HAVf2").text

                            # Проверка валидности ссылки и цены
                            if car_link and (car_link.startswith("http") or car_link.startswith("https")) and car_price:
                                if car_link in downloaded_urls:
                                    # Проверка изменения цены для уже известных URL
                                    if downloaded_urls[car_link] != car_price:
                                        print(f"Изменение цены: {car_link} с {downloaded_urls[car_link]} на {car_price}")
                                        urls.add(car_link)
                                        downloaded_urls[car_link] = car_price  # Обновление цены
                                else:
                                    # Добавление нового URL
                                    urls.add(car_link)
                                    downloaded_urls[car_link] = car_price
                        except Exception as e:
                            # Пропуск объявления при ошибке
                            continue

                    page += 1  # Переход к следующей странице

                print(f"Собрано {len(urls)} URL для модели '{car_model}' марки '{car_mark}'")
                # Сохранение URL для текущей модели
                all_urls[car_model] = urls
            
            # Второй этап: обработка собранных URL для всех моделей
            print(f"Начало детальной обработки URL для марки '{car_mark}'")
            total_processed = 0  # Счетчик обработанных объявлений
            
            # Словарь селекторов для быстрого поиска элементов на странице
            selectors_pool = {
                'title': (By.CLASS_NAME, 'CardHead__title'),  # Заголовок объявления
                'price': (By.CLASS_NAME, 'OfferPriceCaption__price'),  # Цена
                'id_offer': (By.CSS_SELECTOR, '.CardHead__id'),  # ID объявления
                'creation_date_offer': (By.CSS_SELECTOR, '.CardHead__creationDate'),  # Дата создания
                'location': (By.CLASS_NAME, 'MetroListPlace__regionName'),  # Местоположение
                'year': (By.CSS_SELECTOR, '.CardInfoRow_year .CardInfoRow__cell:nth-child(2)'),  # Год выпуска
                'mileage': (By.CSS_SELECTOR, '.CardInfoRow_kmAge .CardInfoRow__cell:nth-child(2)'),  # Пробег
                'engine': (By.CSS_SELECTOR, '.CardInfoRow_engine .CardInfoRow__cell:nth-child(2)'),  # Двигатель
                'transmission': (By.CSS_SELECTOR, '.CardInfoRow_transmission .CardInfoRow__cell:nth-child(2)'),  # Трансмиссия
                'drive': (By.CSS_SELECTOR, '.CardInfoRow_drive .CardInfoRow__cell:nth-child(2)'),  # Привод
                'body_type': (By.CSS_SELECTOR, '.CardInfoRow_bodytype .CardInfoRow__cell:nth-child(2)'),  # Тип кузова
                'color': (By.CSS_SELECTOR, '.CardInfoRow_color .CardInfoRow__cell:nth-child(2)'),  # Цвет
                'owners': (By.CSS_SELECTOR, '.CardInfoRow_ownersCount .CardInfoRow__cell:nth-child(2)'),  # Количество владельцев
                'pts': (By.CSS_SELECTOR, '.CardInfoRow_pts .CardInfoRow__cell:nth-child(2)'),  # ПТС
                'condition': (By.CSS_SELECTOR, '.CardInfoRow_state .CardInfoRow__cell:nth-child(2)'),  # Состояние
                'generation': (By.CSS_SELECTOR, '.CardInfoRow_superGen .CardInfoRow__cell:nth-child(2)'),  # Поколение
                'availability': (By.CSS_SELECTOR, '.CardInfoRow_availability .CardInfoRow__cell:nth-child(2)'),  # Наличие
                'options': (By.CSS_SELECTOR, '.CardInfoRow_complectationOrEquipmentCount .CardInfoRow__cell:nth-child(2)'),  # Опции
                'tax': (By.CSS_SELECTOR, '.CardInfoRow_transportTax .CardInfoRow__cell:nth-child(2)'),  # Налог
                'wheel': (By.CSS_SELECTOR, '.CardInfoRow_wheel .CardInfoRow__cell:nth-child(2)'),  # Руль
                'customs': (By.CSS_SELECTOR, '.CardInfoRow_customs .CardInfoRow__cell:nth-child(2)'),  # Таможня
                'exchange': (By.CSS_SELECTOR, '.CardInfoRow_exchange .CardInfoRow__cell:nth-child(2)'),  # Обмен
            }
            
            def turbo_find_element(driver, field_name, default=None):
                """
                Быстрый поиск элемента на странице с обработкой ошибок
                
                Args:
                    driver: Экземпляр WebDriver
                    field_name (str): Имя поля для поиска (ключ из selectors_pool)
                    default: Значение по умолчанию, если элемент не найден
                    
                Returns:
                    str: Текст найденного элемента или значение по умолчанию
                """
                try:
                    by, value = selectors_pool[field_name]
                    return driver.find_element(by, value).text
                except:
                    return default
            
            # Статистика обработки
            all_models_count = len(all_urls)
            model_counter = 0
            
            # Словарь для хранения успешно обработанных URL
            successful_urls = {}
            
            # Обработка всех URL для каждой модели
            for car_model, urls in all_urls.items():
                model_counter += 1
                print(f"[{model_counter}/{all_models_count}] Обработка {len(urls)} URL для модели '{car_model}'")
                urls_list = list(urls)
                processed_count = 0
                
                # Массив для хранения данных текущей модели
                model_data = []
                
                # Обработка каждого URL
                for i, url in enumerate(urls_list):
                    try:
                        # Открытие страницы объявления
                        driver.get(url)
                        time.sleep(wait_time)
                        
                        # Проверка загрузки страницы (наличие заголовка)
                        try:
                            title = driver.find_element(By.CLASS_NAME, 'CardHead__title').text
                            if not title:
                                continue
                        except:
                            continue
                        
                        # Сбор основных данных об автомобиле
                        data = {
                            'title': title,
                            'mark': car_mark,
                            'model': car_model,
                            'region': region,
                            'url': url,
                        }
                        
                        # Сбор всех дополнительных данных из selectors_pool
                        for field in selectors_pool.keys():
                            if field not in data:  # Пропуск уже добавленных полей
                                data[field] = turbo_find_element(driver, field)
                        
                        # Добавление данных в список
                        model_data.append(data)
                        processed_count += 1
                        total_processed += 1
                        
                        # Добавление URL в список успешных
                        successful_urls[url] = downloaded_urls[url]
                        
                        # Выгрузка в основной массив каждые 1000 записей для экономии памяти
                        if len(model_data) >= 1000:
                            all_data.extend(model_data)
                            model_data = []
                        
                    except Exception as e:
                        # Пропуск при ошибке обработки
                        continue
                    
                    # Вывод прогресса каждые 100 объявлений
                    if (i + 1) % 100 == 0:
                        print(f'Обработано {i + 1} из {len(urls_list)} объявлений модели {car_model}.')
                    
                    # Перезапуск браузера каждые 100 объявлений для экономии памяти
                    if total_processed % 100 == 0:
                        print(f'Перезапуск браузера после {total_processed} объявлений (для оптимизации памяти)')
                        
                        # Пытаемся закрыть текущий браузер
                        try:
                            driver.quit()
                            # Если браузер закрылся успешно, создаем новый экземпляр
                            try:
                                driver = webdriver.Remote(
                                    command_executor='http://selenium-chrome:4444',
                                    options=chrome_options
                                )
                                setup_chrome_optimizations(driver)
                            except Exception as first_error:
                                # При ошибке создания, пробуем еще раз
                                print(f"Ошибка при создании браузера, повторная попытка: {str(first_error)}")
                                driver = webdriver.Remote(
                                    command_executor='http://selenium-chrome:4444',
                                    options=chrome_options
                                )
                                setup_chrome_optimizations(driver)

                        except Exception as e:
                            # Если ошибка при закрытии, продолжаем работу с текущим браузером
                            print(f"Ошибка при закрытии браузера: {str(e)}")
                            print("Продолжение работы с текущим экземпляром браузера")
                
                # Добавление оставшихся данных в общий массив
                all_data.extend(model_data)
                
                # Статистика по обработке модели
                print(f"Модель '{car_model}' обработана. Успешно собрано {processed_count} объявлений")

                # Расчет времени выполнения
                end_time = datetime.now()
                execution_time = end_time - start_time
                print(f"Текущее время выполнения: {execution_time}, всего объявлений: {total_processed}")
            
            # Сохранение успешно обработанных URL для отслеживания изменений
            save_downloaded_urls(car_mark, successful_urls)
            print(f"Сохранено успешных URL: {len(successful_urls)}")
            
            # Обновление счетчика и статистики
            total_cars += total_processed
            print(f"Марка '{car_mark}' полностью обработана. Сохранено {total_processed} объявлений")
            
            # Обновление времени выполнения
            end_time = datetime.now()
            execution_time = end_time - start_time
            print(f"Общее время выполнения: {execution_time}, всего объявлений: {total_cars}")

            # Проверка наличия данных для сохранения
            if len(all_data) == 0: 
                print(f"Нет данных для сохранения по марке '{car_mark}'")
                continue

            # Создание DataFrame из собранных данных
            df = pd.DataFrame(all_data)

            # Сохранение данных в CSV-файл
            csv_path = f'{SAVE_FOLDER_CSV}/car_data_{car_mark}_{region}.csv'
            
            # Проверка существования файла для определения режима записи
            if os.path.exists(csv_path):
                # Добавление к существующему файлу без заголовков
                df.to_csv(csv_path, mode='a', index=False, header=False, encoding='utf-8')
                print(f"Данные добавлены к существующему файлу: {csv_path}")
            else:
                # Создание нового файла с заголовками
                df.to_csv(csv_path, mode='w', index=False, encoding='utf-8')
                print(f"Создан новый файл с данными: {csv_path}")

            # Удаление дублей только если были загружены новые данные
            if total_processed != 0:
                print(f"Начало удаления дублей из файла {csv_path}")
                df = pd.read_csv(csv_path)
                rows_before = len(df)
                df_clean = df.drop_duplicates(subset=['url', 'price'])
                rows_after = len(df_clean)
                df_clean.to_csv(csv_path, mode='w', index=False, encoding='utf-8')
                print(f"Удалено дублей: {rows_before - rows_after}")
        
        print(f'=== Обработка региона {region} успешно завершена ===')

    finally:
        # Закрытие браузера в любом случае
        driver.quit()
        print("Браузер корректно закрыт")
        
        # Расчет итоговой статистики
        end_time = datetime.now()
        execution_time = end_time - start_time
        print(f"Итоговое время выполнения: {execution_time}")
        print(f"Итоговое количество собранных объявлений: {total_cars}")