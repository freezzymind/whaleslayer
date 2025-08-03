import os
import json
import asyncio
import websockets
import asyncpg
from datetime import datetime
from loguru import logger
from partition_manager import manage_partitions


# === Логирование ===
logger.add(
    '/logs/gate_trades_{time:YYYY-MM-DD-HH}.log',
    enqueue=True,
    rotation='10 MB',
    retention='3 days',
    compression='zip'
)

# === Константы ===
WS_URL = 'wss://api.gate.io/ws/v4/'
PAIR = 'ETH_USDT'
TABLE = os.getenv('TRADES')


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


# === Обработка входящих сделок ===
async def handle_msg(msg, conn):
    try:
        data = json.loads(msg)
        if data.get('event') != 'update' or 'result' not in data:
            return

        for trade in data['result']:
            ts = datetime.utcfromtimestamp(int(float(trade['create_time'])))
            trade_id = int(trade['id'])
            price = float(trade['price'])
            qty = float(trade['amount'])
            side = trade['side']

            if side not in ('buy', 'sell'):
                logger.warning('[SKIP] Неизвестный side: {} — trade_id={}', side, trade_id)
                continue

            query = f'''
                INSERT INTO {TABLE} (trade_id, timestamp, price, qty, side)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (timestamp, trade_id) DO NOTHING
            '''
            await conn.execute(query, trade_id, ts, price, qty, side)
            logger.info('[SAVE] id={} side={} price={} qty={}', trade_id, side, price, qty)

    except Exception as e:
        logger.error(f'[PROCESS] Ошибка обработки сделки: {e}')


async def subscribe(conn, WS_URL, PAIR):
    while True:
        try:
            async with websockets.connect(uri=WS_URL, ping_interval=20) as ws:
                await ws.send(json.dumps({
                    'time': 0,
                    'channel': 'spot.trades',
                    'event': 'subscribe',
                    'payload': [PAIR]
                }))
                logger.info('[WS] Подписка на ленту сделок: {}', PAIR)

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        await handle_msg(msg, conn)
                    except asyncio.TimeoutError:
                        logger.warning('[WS] 30s без сообщений — разрыв соединения')
                        break
                    except Exception as e:
                        logger.error(f'[WS] Ошибка при получении сообщения: {e}')
                        break

        except Exception as e:
            logger.error(f'[WS] Ошибка WebSocket-соединения: {e}. Переподключение через 5 секунд...')
            await asyncio.sleep(5)


# === Проверка, создание и удаление партиций раз в сутки ===
async def daily_partition_job(TABLE):
    while True:
        try:
            await manage_partitions(TABLE)
            await asyncio.sleep(86400)
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

        await manage_partitions(TABLE)

        task_subscribe = asyncio.create_task(subscribe(conn, WS_URL, PAIR))
        task_partition = asyncio.create_task(daily_partition_job(TABLE))
        await asyncio.gather(task_subscribe, task_partition)

    except Exception as e:
        logger.critical(f'[FATAL] Непредвиденная ошибка: {e}', exc_info=True)
        raise SystemExit(1)


if __name__ == '__main__':
    asyncio.run(main())
