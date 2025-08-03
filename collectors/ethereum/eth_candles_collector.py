import os
import json
import time
import asyncio
import websockets
from loguru import logger
import asyncpg
from datetime import datetime
from partition_manager import manage_partitions


# === Логирование ===
logger.add(
    '/logs/gate_candles_{time:YYYY-MM-DD-HH}.log',
    enqueue=True,
    rotation='10 MB',
    retention='3 days',
    compression='zip'
)

# === Константы ===
WS_URL = 'wss://api.gateio.ws/ws/v4/'

# === Настройки ===
PAIR = 'ETH_USDT'
INTERVAL = '1m'
TABLE = os.getenv('CANDLES')


# === Подключение к PostgreSQL ===
async def connect_pg():
    try:
        conn = await asyncpg.connect(
            user=os.getenv('COLLECTOR'),
            password=os.getenv('COLLECTOR_PASSWORD'),
            database=os.getenv('POSTGRES_DB'),
            host=os.getenv('DB_HOST')
            )
        logger.info('Успешное подключение к базе данных')
        return conn
    except Exception as e:
        logger.error(f'Ошибка подключения к базе данных: {e}')


# === Подписка на свечи ===
async def subscribe(PAIR: str, conn):
    while True:
        try:
            async with websockets.connect(uri=WS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps({
                    'time': int(time.time()),
                    'channel': 'spot.candlesticks',
                    'event': 'subscribe',
                    'payload': [INTERVAL, PAIR]
                }))
                logger.info('[WS] Subscribed to candles: {} {}', PAIR, INTERVAL)

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        await handle_msg(PAIR, msg, conn)
                    except asyncio.TimeoutError:
                        logger.warning('[TIMEOUT] 30s no message — reconnecting')
                        break
        except Exception as e:
            logger.error('[WS] Connection error: {}', e)
            await asyncio.sleep(5)


# === Обработка свечи ===
async def handle_msg(PAIR: str, msg, conn):
    try:
        data = json.loads(msg)
        if data.get('event') != 'update':
            return

        t, o, h, l, c, v = data['result']['n']
        ts = datetime.utcfromtimestamp(int(t))
        query = f'''
        INSERT INTO {TABLE} (timestamp, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (timestamp) DO NOTHING;
        '''
        await conn.execute(query, ts, float(o), float(h), float(l), float(c), float(v))
        logger.info('[SAVE] {} {} o={} c={}', PAIR, ts, o, c)

    except Exception as e:
        logger.error('[PROCESS] Error parsing message: {}', e)


# Проверка партиций в БД
async def daily_partition_job():
    while True:
        try:
            await manage_partitions(TABLE)
            await asyncio.sleep(86400)  # раз в сутки
        except Exception as e:
            logger.critical(f'[FATAL] Ошибка при создании партиций: {e}')
            raise SystemExit(1)


# === Основной запуск ===
async def main():
    try:
        conn = await connect_pg()
        if not conn or conn.is_closed():
            logger.critical('[FATAL] Подключение к базе данных не удалось.')
            raise SystemExit(1)

        try:
            await manage_partitions(TABLE)
        except Exception as e:
            logger.critical(f'[FATAL] Ошибка при создании партиций: {e}')
            raise SystemExit(1)

        task_candle = asyncio.create_task(subscribe(PAIR, conn))
        task_partition = asyncio.create_task(daily_partition_job())
        await asyncio.gather(task_candle, task_partition)

    except Exception as e:
        logger.critical(f'[FATAL] Непредвиденная ошибка: {e}', exc_info=True)
        raise SystemExit(1)

if __name__ == '__main__':
    asyncio.run(main())
