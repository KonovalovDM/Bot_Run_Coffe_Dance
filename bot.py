import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from datetime import datetime, timedelta
import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Конфигурация
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EVENT_DATE = datetime(2024, 12, 15)  # Дата мероприятия
ADMIN_IDS = [123456789]  # ID администраторов

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect(
        user="postgres", password="password", database="event_bot", host="localhost"
    )
# Пользовательский сценарий
# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="📱 Отправить номер", request_contact=True))
    await message.answer(
        "Добро пожаловать! Регистрируйтесь на мероприятие «Бег, Кофе, Танцы».\n"
        "Пожалуйста, отправьте ваш номер телефона и ник в Telegram.",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )

# Сбор данных (номер телефона + username)
@dp.message(F.contact)
async def handle_contact(message: types.Message):
    phone = message.contact.phone_number
    username = message.from_user.username or "Не указан"

    # Сохранение в БД
    conn = await get_db()
    await conn.execute(
        "INSERT INTO participants (telegram_user_id, username, phone_number, registration_time) "
        "VALUES ($1, $2, $3, $4)",
        message.from_user.id, username, phone, datetime.now()
    )
    await conn.close()

    await message.answer(
        "✅ Спасибо! Вы зарегистрированы на мероприятие «Бег, Кофе, Танцы». "
        "За день до события мы напомним вам о встрече!",
        reply_markup=types.ReplyKeyboardRemove()
    )

# Напоминания (APScheduler)
scheduler = AsyncIOScheduler()


async def send_reminders():
    conn = await get_db()
    participants = await conn.fetch(
        "SELECT telegram_user_id FROM participants "
        "WHERE reminder_sent = False AND registration_time < $1",
        datetime.now() - timedelta(hours=24)
    )

    for participant in participants:
        await bot.send_message(
            participant["telegram_user_id"],
            "⏰ Напоминаем: завтра состоится мероприятие «Бег, Кофе, Танцы»! Ждём вас!"
        )
        await conn.execute(
            "UPDATE participants SET reminder_sent = True WHERE telegram_user_id = $1",
            participant["telegram_user_id"]
        )

    await conn.close()


# Запуск каждые 30 минут
scheduler.add_job(send_reminders, "interval", minutes=30)
scheduler.start()

# Команды Администратора
# /list - список участников
@dp.message(Command("list"), F.from_user.id.in_(ADMIN_IDS))
async def cmd_list(message: types.Message):
    conn = await get_db()
    count = await conn.fetchval("SELECT COUNT(*) FROM participants")
    await message.answer(f"📊 Зарегистрировано участников: {count}")
    await conn.close()

# /export - Выгрузка в CSV
import csv
from io import StringIO


@dp.message(Command("export"), F.from_user.id.in_(ADMIN_IDS))
async def cmd_export(message: types.Message):
    conn = await get_db()
    participants = await conn.fetch("SELECT * FROM participants")

    csv_file = StringIO()
    writer = csv.DictWriter(csv_file, fieldnames=["id", "username", "phone_number"])
    writer.writeheader()
    writer.writerows([dict(record) for record in participants])

    await message.answer_document(
        types.BufferedInputFile(csv_file.getvalue().encode(), filename="participants.csv")
    )
    await conn.close()

# /broadcast - Рассылка сообщений
@dp.message(Command("broadcast"), F.from_user.id.in_(ADMIN_IDS))
async def cmd_broadcast(message: types.Message):
    text = message.text.split(" ", 1)[1]  # Текст после /broadcast
    conn = await get_db()
    participants = await conn.fetch("SELECT telegram_user_id FROM participants")

    for participant in participants:
        try:
            await bot.send_message(participant["telegram_user_id"], text)
        except Exception:
            pass  # Пропуск неактивных пользователей

    await message.answer(f"📢 Сообщение отправлено {len(participants)} участникам")
    await conn.close()

# Схема базы данных (PostgreSQL)
CREATE TABLE participants (
    id SERIAL PRIMARY KEY,
    telegram_user_id BIGINT UNIQUE NOT NULL,
    username VARCHAR(255),
    phone_number VARCHAR(20) NOT NULL,
    registration_time TIMESTAMP DEFAULT NOW(),
    reminder_sent BOOLEAN DEFAULT FALSE
);

# Запуск бота
import asyncio

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())