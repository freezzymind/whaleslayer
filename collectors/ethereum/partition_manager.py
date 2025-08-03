import os
import asyncpg
from loguru import logger
from datetime import datetime, timedelta, timezone


# === Логирование с ротацией и сжатием ===
logger.add(
    '/logs/partition-manager_{time:YYYY-MM-DD-HH}.log',
    enqueue=True,
    rotation='10 MB',
    retention='1 day',
    compression='zip'
)


async def manage_partitions(table: str):
    try:
        conn = await asyncpg.connect(
            user=os.getenv('PARTITION_MANAGER'),
            password=os.getenv('PARTITION_MANAGER_PASSWORD'),
            database=os.getenv('POSTGRES_DB'),
            host=os.getenv('DB_HOST')
        )
        logger.info(f'Подключено к БД как partition_manager для таблицы: {table}')

        today = datetime.now(timezone.utc).date()

        # Make new partitions for tables
        for offset in (0, 1):  # today and tomorrow
            day = today + timedelta(days=offset)
            date_str = day.strftime('%Y%m%d')
            day_start = day.isoformat()
            day_end = (day + timedelta(days=1)).isoformat()
            partition_table = f'{table}_{date_str}'

            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {partition_table}
                PARTITION OF {table}
                FOR VALUES FROM ('{day_start}') TO ('{day_end}');
            ''')
            logger.info(f'Партиция создана (или уже существует): {partition_table}')

        # Drop partition older than 183 days
        old_day = (today - timedelta(days=183)).strftime('%Y%m%d')
        old_partition_table = f'{table}_{old_day}'
        await conn.execute(f'DROP TABLE IF EXISTS {old_partition_table};')
        logger.info(f'Удалена устаревшая партиция (если была): {old_partition_table}')

    except Exception as e:
        logger.error(f'Ошибка в partition manager для {table}: {e}')
    finally:
        if 'conn' in locals():
            await conn.close()
            logger.info(f'Соединение с БД закрыто для таблицы: {table}')
