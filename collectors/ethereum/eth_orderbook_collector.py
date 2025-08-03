import os
import asyncio
import json
import websockets
import asyncpg
import aiohttp
from datetime import datetime
from loguru import logger
from partition_manager import manage_partitions


# === Логирование ===
logger.add(
    '/logs/gate_orderbook_{time:YYYY-MM-DD-HH}.log',
    enqueue=True,
    rotation='10 MB',
    retention='3 days',
    compression='zip'
)


# === Константы ===
WS_URL = 'wss://api.gate.io/ws/v4/'
PAIR = 'ETH_USDT'
DEPTH = 25
INTERVAL = 1  # в секундах
TABLE = os.environ['ORDERBOOK']


# === Глобальный ордербук в памяти ===
local_orderbook = {
    'bids': [],
    'asks': []
}


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


# === Получить snapshot через REST ===
async def get_snapshot() -> bool:
    url = f'https://api.gate.io/api/v4/spot/order_book?currency_pair={PAIR}&limit={DEPTH}'
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.error(f'Ошибка при получении snapshot: статус {resp.status}')
                    return False

                data = await resp.json()
                local_orderbook['bids'] = data.get('bids', [])
                local_orderbook['asks'] = data.get('asks', [])
                if not local_orderbook['bids'] or not local_orderbook['asks']:
                    logger.warning('Получен пустой snapshot — возможны проблемы с API')
                    return False

                logger.info('Получен стартовый snapshot')
                return True

    except aiohttp.ClientError as e:
        logger.error(f'Сетевой сбой при получении snapshot: {e}')
    except asyncio.TimeoutError:
        logger.error('Тайм-аут при получении snapshot')
    except Exception as e:
        logger.error(f'Ошибка при получении snapshot: {e}')
    return False  # если случилось исключение


# === Обновление стакана по диффам ===
def apply_diff(update: dict):
    try:
        for side in ['bids', 'asks']:
            if side not in update:
                continue

            # Преобразуем локальный стакан в словарь
            levels = {price: size for price, size in local_orderbook[side]}

            for level in update[side]:
                if not isinstance(level, list) or len(level) != 2:
                    logger.warning(f'Неверный формат уровня в update[{side}]: {level}')
                    continue
                price, size = level
                try:
                    if float(size) == 0:
                        levels.pop(price, None)
                    else:
                        levels[price] = size
                except Exception as e:
                    logger.warning(f'Ошибка при обновлении уровня {price}: {e}')
                    continue

            # Сортируем по числовой цене и обрезаем до DEPTH
            try:
                sorted_levels = sorted(
                    ((float(price), size) for price, size in levels.items()),
                    key=lambda x: x[0],
                    reverse=(side == 'bids')
                )
                # Возвращаем обратно в строковый формат для JSONB
                local_orderbook[side] = [[f'{price:.8f}', size] for price, size in sorted_levels[:DEPTH]]
            except Exception as e:
                logger.error(f'Ошибка при сортировке {side}: {e}')

    except Exception as e:
        logger.error(f'Ошибка в apply_diff: {e}')


# === Сохранение snapshot в PostgreSQL ===
async def save_snapshot(conn):
    try:
        ts = datetime.utcnow().replace(microsecond=0)
        query = f'''
            INSERT INTO {TABLE} (timestamp, bids, asks)
            VALUES ($1, $2::jsonb, $3::jsonb)
            ON CONFLICT (timestamp) DO NOTHING
        '''
        await conn.execute(
            query,
            ts,
            json.dumps(local_orderbook['bids']),
            json.dumps(local_orderbook['asks'])
        )
        logger.info(f'Snapshot сохранён: {ts}')

    except Exception as e:
        logger.error(f'Ошибка при сохранении snapshot: {e}')


# === Подписка на стакан ===
async def subscribe(WS_URL, PAIR, DEPTH):
    ok = await get_snapshot()
    if not ok:
        logger.critical('Snapshot не получен — прерывание subscribe()')
        return
    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        # Подписка на обновления
        try:
            await ws.send(json.dumps({
                'time': 0,
                'channel': 'spot.order_book_update',
                'event': 'subscribe',
                'payload': [PAIR, str(DEPTH), '100ms']
            }))
            logger.info('Подписка на стакан отправлена')
        except Exception as e:
            logger.error(f'Ошибка при отправке подписки: {e}. Попытка переподключения через 5 секунд...')
            await asyncio.sleep(5)

        # Приём обновлений
        async for msg in ws:
            data = json.loads(msg)
            if data.get('event') == 'update' and 'result' in data:
                apply_diff(data['result'])


# === Периодическое сохранение в БД ===
async def periodic_saver(conn, INTERVAL):
    while True:
        try:
            await asyncio.sleep(INTERVAL)
            await save_snapshot(conn)
        except Exception as e:
            logger.error(f'Ошибка в periodic_saver: {e}')


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

        task_subscribe = asyncio.create_task(subscribe(WS_URL, PAIR, DEPTH))
        task_saver = asyncio.create_task(periodic_saver(conn, INTERVAL))
        task_partition = asyncio.create_task(daily_partition_job(TABLE))
        await asyncio.gather(task_subscribe, task_saver, task_partition)

    except Exception as e:
        logger.critical(f'[FATAL] Непредвиденная ошибка: {e}', exc_info=True)
        raise SystemExit(1)


if __name__ == '__main__':
    asyncio.run(main())
