import asyncio
import time

import asyncpg
import aioredis
import gspread
from bot import init_telegram_bot
from constants import SHEET_NAME, DOC_NAME, CHAT_FOR_MESSAGES
from tracking_changes import *
from tracking_delivery_date import tracking_delivery_date


async def init_redis_conn():
    return await aioredis.StrictRedis(host='localhost', port=6379, decode_responses=True)


async def init_db_conn():
    return await asyncpg.create_pool(user='postgres', host='127.0.0.1', database='supplies')


def init_worksheet():
    sa = gspread.service_account()
    sh = sa.open(DOC_NAME)
    sh.worksheets()
    return sh.worksheet(SHEET_NAME)


async def overwrite_redis(redis, wks):
    for key in await redis.keys(): await redis.delete(key)     #удаляем все ключи
    await redis.rpush(ORDER_ID_COLUMN_NAME, *(wks.col_values(ORDER_ID_COLUMN))) #создаём их снова
    await redis.rpush(ORDER_NUMBER_COLUMN_NAME, *(wks.col_values(ORDER_NUMBER_COLUMN)))
    await redis.rpush(ORDER_PRICE_COLUMN_NAME, *(wks.col_values(ORDER_PRICE_COLUMN)))
    await redis.rpush(DELIVERY_DATE_COLUMN_NAME, *(wks.col_values(DELIVERY_DATE_COLUMN)))


async def main():
    redis = await init_redis_conn() #инициализируем редис
    pool = await init_db_conn() #инициализируем пул соединений с бд
    bot = init_telegram_bot() #инициализируем тг бота
    wks = init_worksheet() #соединение с гугл документом
    alerted_orders = [] #создаем список заказов, о просрочке которых уже отправлялись уведомления
    await overwrite_redis(redis, wks) #перезаписываем ключи в редисе
    await update_data_in_db(redis, pool) # обновляем данные в бд в соответствии с редисом
    while True:
        await tracking_delivery_date(pool, bot, CHAT_FOR_MESSAGES, alerted_orders) # отслеживаем сроки поставки
        await change_tracking(wks, redis, pool) # отслеживаем изменения в документе
        time.sleep(10)


if __name__ == '__main__':
    asyncio.run(main())
