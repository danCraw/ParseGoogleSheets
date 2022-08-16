from datetime import datetime, date

import requests
from pydantic import ValidationError

from classes import Order
from constants import ORDER_ID_COLUMN, ORDER_NUMBER_COLUMN, DELIVERY_DATE_COLUMN, ORDER_PRICE_COLUMN, \
    ORDER_ID_COLUMN_NAME, ORDER_NUMBER_COLUMN_NAME, ORDER_PRICE_COLUMN_NAME, DELIVERY_DATE_COLUMN_NAME


def get_usd_rub_value(): # получение курса доллар - рубль
    data = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()
    return data['Valute']['USD']['Value']


async def update_redis_list(redis, list_name, new_list_data): # перезапись списка в редис
    await redis.delete(list_name)
    await redis.rpush(list_name, *new_list_data)


async def row_in_redis_is_full(redis, row_numer): #проверка на то, заполнены ли все колонки в редисе для конкретной строки
    index = row_numer - 1
    order_serial_number = await redis.lrange(ORDER_ID_COLUMN_NAME, 0, -1)
    order_numbers = await redis.lrange(ORDER_NUMBER_COLUMN_NAME, 0, -1)
    order_prices = await redis.lrange(ORDER_PRICE_COLUMN_NAME, 0, -1)
    delivery_dates = await redis.lrange(DELIVERY_DATE_COLUMN_NAME, 0, -1)
    try:
        if (len(order_serial_number) == len(order_numbers) == len(order_prices) == len(delivery_dates)) \
                and (order_serial_number[index] and order_numbers[index] and order_prices[index] and delivery_dates[index]):
            return True
        else:
            return False
    except IndexError:
        print(f'index not exist in redis {index}')
        return False


async def row_dont_exist_in_redis(redis, row_number): #проверка на то, существует ли колонка с данным индексом в редисе
    index = row_number - 1
    order_serial_number = await redis.lrange(ORDER_ID_COLUMN_NAME, 0, -1)
    order_numbers = await redis.lrange(ORDER_NUMBER_COLUMN_NAME, 0, -1)
    order_prices = await redis.lrange(ORDER_PRICE_COLUMN_NAME, 0, -1)
    delivery_dates = await redis.lrange(DELIVERY_DATE_COLUMN_NAME, 0, -1)
    try:
        if (len(order_serial_number) == len(order_numbers) == len(order_prices) == len(delivery_dates)) \
                and (order_serial_number[index] and order_numbers[index] and order_prices[index] and delivery_dates[index]):
            return False
        else:
            return True
    except IndexError:
        return True


async def get_order_from_db(conn_pool, row_number): # получаем заказ из базы
    async with conn_pool.acquire() as con:
        order = await con.fetchrow(
            '''SELECT number, order_number, price_usd, delivery_date  
            FROM orders
            WHERE row_number = $1;''', row_number)
    return order


async def delete_order_from_db(conn_pool, row_number): # удаляем заказ из базы
    async with conn_pool.acquire() as con:
        await con.execute('''DELETE FROM orders
                             WHERE row_number = $1;''', row_number)


async def update_order_in_db(conn_pool, row_number, col_for_update, update_data):
    # формируем update запрос в базу на основании полученных данных
    rows_to_update = ''
    first = True
    for c in col_for_update:
        if not first:
            rows_to_update += ', '
        first = False
        rows_to_update += c
    new_data = ''
    first = True
    for d in update_data:
        if not first:
            new_data += ', '
        first = False
        if type(d) == date:
            d = f'TO_DATE{d.strftime("%d.%m.%Y"), "DD.MM.YYYY"}'
        new_data += str(d)
    if len(col_for_update) > 1:
        set_data = f'SET({rows_to_update}) = ({new_data})'
    else:
        set_data = f'SET {rows_to_update} = {new_data}'
    async with conn_pool.acquire() as con:
        await con.execute(f'''UPDATE orders
                             {set_data}
                             WHERE row_number = $1;''', row_number)


async def add_order_in_db(conn_pool, row_number, order: Order): # добавляем заказ в бд
    async with conn_pool.acquire() as con:
        await con.execute('''INSERT INTO orders (row_number, number, order_number, price_usd, price_rub, delivery_date)
     VALUES ($1, $2, $3, $4, $5, $6);''', row_number, order.number, order.order_number, order.price_usd,
                          order.price_usd * get_usd_rub_value(), order.delivery_date)


async def get_order_from_row(redis, row_number): # формируем заказ на основе строк из таблицы
    index = row_number - 2
    order_serial_number = await redis.lrange(ORDER_ID_COLUMN_NAME, 1, -1)
    order_numbers = await redis.lrange(ORDER_NUMBER_COLUMN_NAME, 1, -1)
    order_prices = await redis.lrange(ORDER_PRICE_COLUMN_NAME, 1, -1)
    delivery_dates = await redis.lrange(DELIVERY_DATE_COLUMN_NAME, 1, -1)
    try:
        return {'number': order_serial_number[index], 'order_number': order_numbers[index],
                'price_usd': order_prices[index],
                'delivery_date': datetime.strptime(delivery_dates[index], "%d.%m.%Y")}
    except Exception as e:
        print(e)


async def get_data_for_update_in_db(row_in_db: dict, order: Order): # находим колонки и данные в бд, которые нужно обновить
    diff = dict(set(order.dict().items() ^ set(row_in_db.items())))
    return diff.keys(), [order.dict()[key] for key in diff.keys()]


async def update_data_in_db(redis, conn_pool): # обновляем бд до актуального состояния
    orders = await redis.lrange(ORDER_ID_COLUMN_NAME, 0, -1)
    for row_number in range(2, len(orders) + 1):
        # проходимся по строкам документа, ориентируясь на длинну первой колонки.
        # Начинаем с цифры 2 тк данные идут в таблице со 2 строки
        row_in_db = await get_order_from_db(conn_pool, row_number)
        if await row_in_redis_is_full(redis, row_number):
            try:
                order = Order.parse_obj(await get_order_from_row(redis, row_number))
                if row_in_db is None: # если колонки под указанным номером нет бд, то добавляем
                    await add_order_in_db(conn_pool, row_number, order)
                elif dict(row_in_db) != order.dict(): # если данные бд не актуальны
                    col_names, data_for_update = await get_data_for_update_in_db(row_in_db, order)
                    print(f'обновление {row_number}')
                    await update_order_in_db(conn_pool, row_number, col_names, data_for_update)
            except ValidationError as e:
                print(e, f'row {row_number}')
        if await row_dont_exist_in_redis(redis, row_number) and row_in_db is not None: # если в бд лежат удалённые данные
            print(f'delete row {row_number}')
            await delete_order_from_db(conn_pool, row_number)
    last_row = len(orders) + 1 # удаление для последней строки
    row_in_db = await get_order_from_db(conn_pool, last_row)
    if await row_dont_exist_in_redis(redis, last_row) and row_in_db is not None:
        print(f'delete row {last_row}')
        await delete_order_from_db(conn_pool, last_row)


async def add_order_from_redis_to_db(redis, conn_pool, row_number): #добавление заказа со всеми данными из редиса в бд
    if await row_in_redis_is_full(redis, row_number):
        try:
            order = await get_order_from_row(redis, row_number)
            print(row_number)
            await add_order_in_db(conn_pool, row_number, Order.parse_obj(order))
            print('добавление в базу')
        except (ValueError, IndexError):
            pass


async def change_data_in_db(redis, conn_pool, changes):
    print(changes)
    if changes[0]: # если были внесены изменения в данные
        row_number = changes[0].get('row')
        row_in_db = await get_order_from_db(conn_pool, row_number)
        if row_in_db:
            try:
                order = Order.parse_obj(await get_order_from_row(redis, row_number))
                col_for_update, data_for_update = await get_data_for_update_in_db(await get_order_from_db(conn_pool, row_number),
                                                                                  order)
                if col_for_update:
                    await update_order_in_db(conn_pool, row_number, col_for_update, data_for_update)
                    print('изменения в базе')
            except ValidationError as e:
                print(e)
        else: # если строки для изменений в базе нет,выполняем добавление
            await add_order_from_redis_to_db(redis, conn_pool, row_number)
    if changes[1]: # если данные были добавлены
        row_number = changes[1].get('row')
        await add_order_from_redis_to_db(redis, conn_pool, row_number)
    if changes[2]: # если данные были удалены
        if await row_dont_exist_in_redis(redis, changes[2].get('row')):
            await delete_order_from_db(conn_pool, changes[2].get('row'))
            print('удаление из базы')


async def get_changed_elements(redis, new_list, old_list):
    changed_elements = {}
    added_elements = {}
    deleted_elements = {}
    for i in range(1, len(new_list)): # определяем тип изменений
        try:
            if (new_list[i] != old_list[i]) and (len(new_list) == len(old_list)) and new_list[i] and old_list[i]\
                    and new_list[i] != 0 and not await row_dont_exist_in_redis(redis, i+1):
                changed_elements.update({'update': f'{new_list[i], old_list[i]}', 'row': i + 1}) # произошло изменение
            if (new_list[i] != old_list[i]) and (len(new_list) == len(old_list)) and new_list[i] and not old_list[i]\
                    and new_list[i] != 0 and old_list[i] != 0 and await row_dont_exist_in_redis(redis, i+1):
                added_elements.update({'add': f'{new_list[i]}', 'row': i + 1}) # произошло добавление
        except IndexError:
            added_elements.update({'add': f'{new_list[i]}', 'row': i + 1})
    for i in range(len(old_list)):
        try:
            if not new_list[i] and new_list[i] != 0:
                deleted_elements.update({'delete': f'{old_list[i]}', 'row': i + 1})
        except IndexError:
            deleted_elements.update({'delete': f'{old_list[i]}', 'row': i + 1}) # произошло удаление
    return changed_elements, added_elements, deleted_elements


async def column_change_tracking(wks, redis, conn_pool, column_name):
    tracking_column = {
        ORDER_ID_COLUMN_NAME: ORDER_ID_COLUMN,
        ORDER_NUMBER_COLUMN_NAME: ORDER_NUMBER_COLUMN,
        ORDER_PRICE_COLUMN_NAME: ORDER_PRICE_COLUMN,
        DELIVERY_DATE_COLUMN_NAME: DELIVERY_DATE_COLUMN,
    }  # словарь с именами и номерами отслеживаемых колонок
    new_old_column_data = wks.col_values(tracking_column.get(column_name))
    old_old_column_data = await redis.lrange(column_name, 0, -1)  # колонка со старыми данными из редиса и новыми из документа
    if new_old_column_data != old_old_column_data:
        print('new', new_old_column_data)
        print('old', old_old_column_data)
        print(f'изменения в {column_name}')
        await update_redis_list(redis, column_name, new_old_column_data) #обновляем данные в редисе
        changes = await get_changed_elements(redis, new_old_column_data, old_old_column_data) # получаем сделанные изменения
        if changes:
            await change_data_in_db(redis, conn_pool, changes) #записываем изменения в бд


async def change_tracking(wks, redis, conn_pool):
    print('changes_tracking...') # отслеживаем все колонки
    await column_change_tracking(wks, redis, conn_pool, ORDER_ID_COLUMN_NAME)
    await column_change_tracking(wks, redis, conn_pool, ORDER_NUMBER_COLUMN_NAME)
    await column_change_tracking(wks, redis, conn_pool, ORDER_PRICE_COLUMN_NAME)
    await column_change_tracking(wks, redis, conn_pool, DELIVERY_DATE_COLUMN_NAME)
