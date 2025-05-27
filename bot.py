import os
import logging
import json
import csv
import asyncio
from io import StringIO
from datetime import datetime, timedelta
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Загрузка конфигурации
with open("config.json", "r", encoding="utf-8") as config_file:
    CONFIG = json.load(config_file)

# Загрузка текстовых сообщений
with open("messages.json", "r", encoding="utf-8") as messages_file:
    MESSAGES = json.load(messages_file)

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

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

    conn = None
    try:
        conn = await get_db()
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
    except Exception as e:
        logging.error(f"Ошибка регистрации: {e}")
        await message.answer("Произошла ошибка при регистрации")
    finally:
        if conn:
            await conn.close()

# Напоминания
async def send_reminders():
    conn = None
    try:
        conn = await get_db()
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
    except Exception as e:
        logging.error(f"Ошибка при работе с БД в send_reminders: {e}")
    finally:
        if conn:
            await conn.close()

# Команды администратора
@dp.message(Command("list"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_list(message: types.Message):
    conn = None
    try:
        conn = await get_db()
        count = await conn.fetchval("SELECT COUNT(*) FROM participants")
        await message.answer(MESSAGES["admin"]["list"].format(count))

        if conn == 0:
            await message.answer(MESSAGES["admin"]["db_empty"])
        else:
            participants = await conn.fetch("SELECT * FROM participants LIMIT 10")
            data = "\n".join(
                [f"ID: {p['id']}, User: @{p['username']}, Phone: {p['phone_number']}"
                 for p in participants]
            )
            await message.answer(MESSAGES["admin"]["list"].format(count))
            await message.answer(MESSAGES["admin"]["db_data"].format(data=data))

    except Exception as e:
        logging.error(f"Ошибка при получении списка участников: {e}")
        await message.answer("Произошла ошибка при получении данных")
    finally:
        if conn:
            await conn.close()

@dp.message(Command("export"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_export(message: types.Message):
    conn = None
    try:
        conn = await get_db()
        participants = await conn.fetch("SELECT * FROM participants")

        if not participants:
            await message.answer(MESSAGES["admin"]["db_empty"])
            return

        csv_file = StringIO()
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["id", "username", "phone_number", "registration_time"]
        )
        writer.writeheader()
        writer.writerows([dict(record) for record in participants])

        await message.answer_document(
            types.BufferedInputFile(
                csv_file.getvalue().encode(),
                filename="participants.csv"
            )
        )
        await message.answer(MESSAGES["admin"]["export_success"])
    except Exception as e:
        logging.error(f"Ошибка при экспорте данных: {e}")
        await message.answer("Произошла ошибка при экспорте данных")
    finally:
        if conn:
            await conn.close()

@dp.message(Command("broadcast"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_broadcast(message: types.Message):
    conn = None
    try:
        text = message.text.split(" ", 1)[1]
        conn = await get_db()
        participants = await conn.fetch("SELECT telegram_user_id FROM participants")

        if not participants:
            await message.answer(MESSAGES["admin"]["db_empty"])
            return

        success = 0
        for participant in participants:
            try:
                await bot.send_message(participant["telegram_user_id"], text)
                success += 1
            except Exception as e:
                logging.error(f"Ошибка отправки сообщения пользователю {participant['telegram_user_id']}: {e}")
                continue

        await message.answer(
            MESSAGES["admin"]["broadcast_success"].format(success)
        )
    except IndexError:
        await message.answer("Использование: /broadcast <текст>")
    except Exception as e:
        logging.error(f"Ошибка при рассылке сообщений: {e}")
        await message.answer("Произошла ошибка при рассылке сообщений")
    finally:
        if conn:
            await conn.close()

# Основная функция
async def main():
    # Запускаем планировщик только после старта event loop
    scheduler.add_job(send_reminders, "interval", minutes=30)
    scheduler.start()

    try:
        await dp.start_polling(bot)
    finally:
        # Корректное завершение работы планировщика
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())