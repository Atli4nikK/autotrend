-- Создание таблицы для сырых данных Auto.ru
DROP TABLE IF EXISTS stg_autotrend.autoru_offers;

CREATE TABLE IF NOT EXISTS stg_autotrend.autoru_offers (
    -- Идентификаторы
    id_offer              VARCHAR,                    -- ID объявления
    url                  VARCHAR,                    -- URL объявления
    
    -- Основная информация
    title                VARCHAR,                    -- Заголовок объявления
    price                VARCHAR,                    -- Цена (в текстовом формате)
    
    -- Информация об автомобиле
    mark                 VARCHAR,                    -- Марка
    model                VARCHAR,                    -- Модель
    year                 VARCHAR,                    -- Год выпуска
    mileage             VARCHAR,                    -- Пробег
    engine              VARCHAR,                    -- Двигатель (объем/мощность/тип)
    transmission        VARCHAR,                    -- Коробка передач
    drive               VARCHAR,                    -- Привод
    body_type           VARCHAR,                    -- Тип кузова
    color               VARCHAR,                    -- Цвет
    owners              VARCHAR,                    -- Количество владельцев
    pts                 VARCHAR,                    -- Тип ПТС
    condition           VARCHAR,                    -- Состояние
    generation          VARCHAR,                    -- Поколение
    availability        VARCHAR,                    -- Наличие
    options             VARCHAR,                    -- Опции
    tax                 VARCHAR,                    -- Налог
    wheel               VARCHAR,                    -- Руль
    customs             VARCHAR,                    -- Таможня
    exchange            VARCHAR,                    -- Обмен
    
    -- Локация
    region              VARCHAR,                    -- Регион
    location            VARCHAR,                    -- Город
    
    -- Метаданные
    creation_date_offer VARCHAR,                    -- Дата создания объявления
    
    -- Технические поля
    load_date          TIMESTAMP DEFAULT NOW(),    -- Время загрузки данных
    load_id            VARCHAR                     -- Идентификатор загрузки
);

-- Комментарии к таблице
COMMENT ON TABLE stg_autotrend.autoru_offers IS 'Сырые данные объявлений с Auto.ru';

-- Комментарии к полям
COMMENT ON COLUMN stg_autotrend.autoru_offers.id_offer IS 'Уникальный идентификатор объявления';
COMMENT ON COLUMN stg_autotrend.autoru_offers.url IS 'URL объявления на Auto.ru';
COMMENT ON COLUMN stg_autotrend.autoru_offers.title IS 'Заголовок объявления';
COMMENT ON COLUMN stg_autotrend.autoru_offers.price IS 'Цена автомобиля в текстовом формате';
COMMENT ON COLUMN stg_autotrend.autoru_offers.mark IS 'Марка автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.model IS 'Модель автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.year IS 'Год выпуска автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.mileage IS 'Пробег автомобиля в километрах';
COMMENT ON COLUMN stg_autotrend.autoru_offers.engine IS 'Характеристики двигателя (объем/мощность/тип)';
COMMENT ON COLUMN stg_autotrend.autoru_offers.transmission IS 'Тип коробки передач';
COMMENT ON COLUMN stg_autotrend.autoru_offers.drive IS 'Тип привода';
COMMENT ON COLUMN stg_autotrend.autoru_offers.body_type IS 'Тип кузова автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.color IS 'Цвет автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.owners IS 'Количество владельцев автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.pts IS 'Тип ПТС (Оригинал/Дубликат)';
COMMENT ON COLUMN stg_autotrend.autoru_offers.condition IS 'Состояние автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.generation IS 'Поколение модели';
COMMENT ON COLUMN stg_autotrend.autoru_offers.availability IS 'Наличие автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.options IS 'Дополнительные опции автомобиля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.tax IS 'Размер транспортного налога';
COMMENT ON COLUMN stg_autotrend.autoru_offers.wheel IS 'Расположение руля';
COMMENT ON COLUMN stg_autotrend.autoru_offers.customs IS 'Статус таможенного оформления';
COMMENT ON COLUMN stg_autotrend.autoru_offers.exchange IS 'Возможность обмена';
COMMENT ON COLUMN stg_autotrend.autoru_offers.region IS 'Регион размещения объявления';
COMMENT ON COLUMN stg_autotrend.autoru_offers.location IS 'Город размещения объявления';
COMMENT ON COLUMN stg_autotrend.autoru_offers.creation_date_offer IS 'Дата создания объявления';
COMMENT ON COLUMN stg_autotrend.autoru_offers.load_date IS 'Время загрузки данных в STG слой';
COMMENT ON COLUMN stg_autotrend.autoru_offers.load_id IS 'Идентификатор загрузки данных'; 