from dataclasses import dataclass
from datetime import date
from pydantic import BaseModel


class Order(BaseModel):
    number: int
    order_number: str
    price_usd: int
    delivery_date: date
