-- Создание схемы для биржи
CREATE SCHEMA IF NOT EXISTS gate;

-- Создание ENUM-типа для side (фиксированные значения)
CREATE TYPE trade_side AS ENUM ('buy', 'sell', 'b', 's');

-- Sequence для trade_id (на случай вставки без внешнего id)
CREATE SEQUENCE IF NOT EXISTS gate.btc_trades_seq;

-- ====== Основные таблицы (мастера) ======

-- Таблица свечей (candles) — партиционирование по дню, PK = timestamp
CREATE TABLE IF NOT EXISTS gate.btc_candles (
    timestamp TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL
) PARTITION BY RANGE (timestamp);

-- Таблица ордербуков — партиционирование по дню, PK = timestamp
CREATE TABLE IF NOT EXISTS gate.btc_orderbook (
    timestamp TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL
) PARTITION BY RANGE (timestamp);

-- Таблица сделок (трейдов) — партиционирование по дню, PK = trade_id (unique по всей таблице)
CREATE TABLE IF NOT EXISTS gate.btc_trades (
    trade_id BIGINT NOT NULL DEFAULT nextval('gate.btc_trades_seq'),
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (timestamp, trade_id),
    price DOUBLE PRECISION NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    side trade_side NOT NULL
) PARTITION BY RANGE (timestamp);

-- ====== Индексы для trades ======
-- Для всех новых партиций индекс создается автоматически, если создать на мастер-таблице:
CREATE INDEX IF NOT EXISTS idx_btc_trades_timestamp ON gate.btc_trades (timestamp);

-- ====== Создание пользователей ======
CREATE USER collector WITH PASSWORD '${COLLECTOR_PASSWORD}';
CREATE USER analyst WITH PASSWORD '${ANALYST_PASSWORD}';
CREATE USER partition_manager WITH PASSWORD '${PARTITION_MANAGER_PASSWORD}';
CREATE USER reader WITH PASSWORD '${READER_PASSWORD}';

-- ====== Права доступа ======

-- Админ
GRANT ALL PRIVILEGES ON SCHEMA gate TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gate TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gate TO postgres;

-- Collector: вставка + чтение (для дедупликации)
GRANT USAGE ON SCHEMA gate TO collector;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA gate TO collector;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gate TO collector;

-- Analyst: только чтение + создание TEMP таблиц
GRANT USAGE ON SCHEMA gate TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA gate TO analyst;
GRANT TEMP ON DATABASE whaleslayersdb TO analyst;

-- Partition_manager: удаление, исправление данных (дедупликация), создание и удаление таблиц
GRANT USAGE ON SCHEMA gate TO partition_manager;
GRANT SELECT, DELETE, UPDATE ON ALL TABLES IN SCHEMA gate TO partition_manager;
GRANT CREATE ON SCHEMA gate TO partition_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gate TO partition_manager;

-- Reader: только просмотр
GRANT USAGE ON SCHEMA gate TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gate TO reader;

-- ====== Наследование прав для будущих объектов ======
ALTER DEFAULT PRIVILEGES IN SCHEMA gate
GRANT INSERT, SELECT ON TABLES TO collector;

ALTER DEFAULT PRIVILEGES IN SCHEMA gate
GRANT USAGE, SELECT ON SEQUENCES TO collector, partition_manager;

ALTER DEFAULT PRIVILEGES IN SCHEMA gate
GRANT SELECT, DELETE, UPDATE ON TABLES TO partition_manager;

ALTER DEFAULT PRIVILEGES IN SCHEMA gate
GRANT CREATE ON SCHEMA TO partition_manager;

ALTER DEFAULT PRIVILEGES IN SCHEMA gate
GRANT SELECT ON TABLES TO reader, analyst;

ALTER DEFAULT PRIVILEGES IN SCHEMA gate
GRANT TEMP ON DATABASE ${POSTGRES_DB} TO analyst;

