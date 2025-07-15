\connect ${POSTGRES_DB}

-- Создание схемы для биржи
CREATE SCHEMA IF NOT EXISTS ${SCHEMA};

-- Создание ENUM-типа для side (фиксированные значения)
CREATE TYPE ${SCHEMA}.trade_side AS ENUM ('buy', 'sell', 'b', 's');

-- Sequence для trade_id (на случай вставки без внешнего id)
CREATE SEQUENCE IF NOT EXISTS ${SEQ_NAME};

-- ====== Основные таблицы (мастера) ======

-- Таблица свечей (candles) — партиционирование по дню, PK = timestamp
CREATE TABLE IF NOT EXISTS ${CANDLES} (
    timestamp TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL
) PARTITION BY RANGE (timestamp);

-- Таблица ордербуков — партиционирование по дню, PK = timestamp
CREATE TABLE IF NOT EXISTS ${ORDERBOOK} (
    timestamp TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL
) PARTITION BY RANGE (timestamp);

-- Таблица сделок (трейдов) — партиционирование по дню, PK = trade_id (unique по всей таблице)
CREATE TABLE IF NOT EXISTS ${TRADES} (
    trade_id BIGINT NOT NULL DEFAULT nextval('${SEQ_NAME}'),
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (timestamp, trade_id),
    price DOUBLE PRECISION NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    side ${SCHEMA}.trade_side NOT NULL
) PARTITION BY RANGE (timestamp);

-- ====== Индексы для trades ======
-- Для всех новых партиций индекс создается автоматически, если создать на мастер-таблице:
CREATE INDEX IF NOT EXISTS ${INDEX} ON ${TRADES} (timestamp);

-- ====== Создание пользователей ======
CREATE USER collector WITH PASSWORD '${COLLECTOR_PASSWORD}';
CREATE USER analyst WITH PASSWORD '${ANALYST_PASSWORD}';
CREATE USER partition_manager WITH PASSWORD '${PARTITION_MANAGER_PASSWORD}';
CREATE USER reader WITH PASSWORD '${READER_PASSWORD}';

-- ====== Права доступа ======

-- Админ
GRANT ALL PRIVILEGES ON SCHEMA ${SCHEMA} TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ${SCHEMA} TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ${SCHEMA} TO postgres;

-- Collector: вставка + чтение (для дедупликации)
GRANT USAGE ON SCHEMA ${SCHEMA} TO collector;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA ${SCHEMA} TO collector;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ${SCHEMA} TO collector;

-- Analyst: только чтение + создание TEMP таблиц
GRANT USAGE ON SCHEMA ${SCHEMA} TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA ${SCHEMA} TO analyst;
GRANT TEMP ON DATABASE ${POSTGRES_DB} TO analyst;

-- Partition_manager: удаление, исправление данных (дедупликация), создание и удаление таблиц
GRANT USAGE ON SCHEMA ${SCHEMA} TO partition_manager;
GRANT SELECT, DELETE, UPDATE ON ALL TABLES IN SCHEMA ${SCHEMA} TO partition_manager;
GRANT CREATE ON SCHEMA ${SCHEMA} TO partition_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ${SCHEMA} TO partition_manager;

-- Reader: только просмотр
GRANT USAGE ON SCHEMA ${SCHEMA} TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ${SCHEMA} TO reader;

-- ====== Наследование прав для будущих объектов ======
ALTER DEFAULT PRIVILEGES IN SCHEMA ${SCHEMA}
GRANT INSERT, SELECT ON TABLES TO collector;

ALTER DEFAULT PRIVILEGES IN SCHEMA ${SCHEMA}
GRANT USAGE, SELECT ON SEQUENCES TO collector, partition_manager;

ALTER DEFAULT PRIVILEGES IN SCHEMA ${SCHEMA}
GRANT SELECT, DELETE, UPDATE ON TABLES TO partition_manager;

ALTER DEFAULT PRIVILEGES IN SCHEMA ${SCHEMA}
GRANT SELECT ON TABLES TO reader, analyst;
