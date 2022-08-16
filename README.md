# ParseGoogleSheets
Парсинг данных с помощью Google Sheets API, запись их в базу и отправка сообщения в телеграмм в случае истечения срока доставки
Гугл документ - https://docs.google.com/spreadsheets/d/1sZ4D83zen7rlvz2rOndPFJNWI52NJ3NQQF5vAhXtvos/edit#gid=0
Python 3.10
PostgreSQL 14.3
psql -U postgres                                                                                                                                                                                               ✔  7h 41m 44s  
CREATE DATABASE supplies;
\c supplies
CREATE TABLE orders (
    row_number integer PRIMARY KEY NOT NULL,
    number integer NOT NULL,
    order_number varchar NOt NULL,
    price_usd integer NOT NULL,
    price_rub numeric NOT NULL,
    delivery_date date NOT NULL
);
pip install asyncio
pip install asyncpg
pip install aioredis
pip install telebot
