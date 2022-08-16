CREATE DATABASE supplies;

CREATE TABLE orders (
    row_number integer PRIMARY KEY NOT NULL,
    number integer NOT NULL,
    order_number varchar NOt NULL,
    price_usd integer NOT NULL,
    price_rub numeric NOT NULL,
    delivery_date date NOT NULL
);