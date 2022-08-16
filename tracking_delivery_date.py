from datetime import date, datetime

from bot import delivery_expiration_send_message


def delivery_time_expired(delivery_date: date):
    if delivery_date < datetime.now().date():
        return True


async def get_order_delivery_dates_fro_db(conn_pool):
    async with conn_pool.acquire() as con:
        orders = await con.fetch(
            '''SELECT delivery_date, order_number
            FROM orders;''')
    return orders


async def tracking_delivery_date(conn_pool, bot, chat_id, alerted_orders):
    orders_deliveres = await get_order_delivery_dates_fro_db(conn_pool) #получаем поставки из базы
    for order in orders_deliveres:
        if delivery_time_expired(order['delivery_date']) and order['order_number'] not in alerted_orders: #если поставка просрочена
            delivery_expiration_send_message(bot, chat_id, order['order_number'])  # и по ней не было ещё прислано уведомление, то шлём его
            alerted_orders.append(order['order_number'])