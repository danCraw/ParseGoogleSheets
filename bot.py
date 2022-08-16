from telebot import TeleBot


def delivery_expiration_send_message(bot: TeleBot, chat_id, order_number):
    bot.send_message(chat_id, f'Доставка заказа {order_number} просрочена')


def init_telegram_bot():
    token = '5388505033:AAFrr4eWVDsX8pP_F9N9_MLyO7fumIq_G7g'
    bot = TeleBot(token)
    return bot