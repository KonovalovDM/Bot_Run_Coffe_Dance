import os
import logging
import json
import csv
from io import StringIO
from datetime import datetime, timedelta
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from apscheduler.executors import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Загрузка конфигурации
with open("config.json", "r", encoding="utf-8") as config_file:
    CONFIG = json.load(config_file)

# Загрузка текстовых сообщений
with open("messages.json", "r", encoding="utf-8") as messages_file:
    MESSAGES = json.load(messages_file)

# Настройка логгирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота
bot = Bot(token=CONFIG["BOT_TOKEN"])
dp = Dispatcher()

# Настройка планировщика
scheduler = AsyncIOScheduler()

# Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect(
        user=CONFIG["DB_USER"],
        password=CONFIG["DB_PASSWORD"],
        database=CONFIG["DB_NAME"],
        host=CONFIG["DB_HOST"]
    )

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="📱 Отправить номер", request_contact=True))
    await message.answer(
        MESSAGES["start"],
        reply_markup=builder.as_markup(resize_keyboard=True)
    )

# Обработка контакта
@dp.message(F.contact)
async def handle_contact(message: types.Message):
    phone = message.contact.phone_number
    username = message.from_user.username or "Не указан"

    async with await get_db() as conn:
        await conn.execute(
            """INSERT INTO participants 
            (telegram_user_id, username, phone_number) 
            VALUES ($1, $2, $3)""",
            message.from_user.id, username, phone
        )

    await message.answer(
        MESSAGES["registration_success"],
        reply_markup=types.ReplyKeyboardRemove()
    )

# Напоминания
async def send_reminders():
    async with await get_db() as conn:
        participants = await conn.fetch(
            """SELECT telegram_user_id FROM participants 
            WHERE reminder_sent = False 
            AND registration_time < NOW() - INTERVAL '24 hours'"""
        )

        for participant in participants:
            try:
                await bot.send_message(
                    participant["telegram_user_id"],
                    MESSAGES["reminder"]
                )
                await conn.execute(
                    """UPDATE participants SET reminder_sent = True 
                    WHERE telegram_user_id = $1""",
                    participant["telegram_user_id"]
                )
            except Exception as e:
                logging.error(f"Ошибка отправки напоминания: {e}")

# Команды администратора
@dp.message(Command("list"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_list(message: types.Message):
    async with await get_db() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM participants")
    await message.answer(MESSAGES["admin"]["list"].format(count))

@dp.message(Command("export"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_export(message: types.Message):
    async with await get_db() as conn:
        participants = await conn.fetch("SELECT * FROM participants")

    csv_file = StringIO()
    writer = csv.DictWriter(csv_file, fieldnames=["id", "username", "phone_number"])
    writer.writeheader()
    writer.writerows([dict(record) for record in participants])

    await message.answer_document(
        types.BufferedInputFile(
            csv_file.getvalue().encode(),
            filename="participants.csv"
        )
    )
    await message.answer(MESSAGES["admin"]["export_success"])

@dp.message(Command("broadcast"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_broadcast(message: types.Message):
    text = message.text.split(" ", 1)[1]
    async with await get_db() as conn:
        participants = await conn.fetch("SELECT telegram_user_id FROM participants")

    success = 0
    for participant in participants:
        try:
            await bot.send_message(participant["telegram_user_id"], text)
            success += 1
        except Exception:
            continue

    await message.answer(
        MESSAGES["admin"]["broadcast_success"].format(success)
    )

# Запуск планировщика
scheduler.add_job(send_reminders, "interval", minutes=30)
scheduler.start()

# Основная функция
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())