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

temp_storage = {}  # Глобальное временное хранилище
confirm_clear = {} # Глобальная переменная для хранения состояния подтверждения удаления данных

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

# Обработка контакта с запросом username, если его нет
@dp.message(F.contact)
async def handle_contact(message: types.Message):
    phone = message.contact.phone_number
    username = message.from_user.username

    # Если username отсутствует, просим ввести вручную
    if not username:
        # Сохраняем номер во временное хранилище
        temp_storage[message.from_user.id] = {"phone": phone}
        await message.answer(
            "Пожалуйста, введите ваш @username вручную:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return

    # Сохраняем данные
    await save_participant(message.from_user.id, username, phone)
    await message.answer(
        MESSAGES["registration_success"],
        reply_markup=types.ReplyKeyboardRemove()
    )

# Обработка ручного ввода username
@dp.message(F.text & ~F.text.startswith('/'))
async def handle_username_input(message: types.Message):
    # Проверяем, что это ответ на запрос username
    if not (message.reply_to_message and
            "введите ваш @username" in message.reply_to_message.text):
        return

    username = message.text.strip('@')  # Удаляем @, если пользователь его ввел
    phone = None  # Номер будем получать из предыдущего сообщения

    # Здесь реализована логика сохранения username
    # Можно использовать временное хранилище или БД для связи с номером телефона
    temp_storage = {}

    # В обработчике контакта
    if not username:
        temp_storage[message.from_user.id] = {"phone": phone}
        await message.answer("Введите @username:")
        return

    # В обработчике username
    if message.from_user.id in temp_storage:
        data = temp_storage.pop(message.from_user.id)
        await save_participant(message.from_user.id, username, data["phone"])

    await message.answer("Спасибо! Ваш username сохранен.")

# Функция сохранения участника в БД
async def save_participant(user_id: int, username: str, phone: str):
    conn = None
    try:
        conn = await get_db()
        await conn.execute(
            """INSERT INTO participants 
            (telegram_user_id, username, phone_number) 
            VALUES ($1, $2, $3)""",
            user_id, username, phone
        )
    except Exception as e:
        logging.error(f"Ошибка сохранения участника: {e}")
        raise
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
        # Получаем соединение с БД
        conn = await get_db()

        # Запрашиваем всех участников из базы данных
        participants = await conn.fetch("""
            SELECT id, telegram_user_id, username, phone_number, 
                   registration_time AT TIME ZONE 'Europe/Moscow' AS registration_time,
                   reminder_sent
            FROM participants
            ORDER BY registration_time DESC
        """)

        # Проверяем, есть ли данные для экспорта
        if not participants:
            await message.answer(MESSAGES["admin"]["db_empty"])
            return

        # Создаем CSV-файл в памяти
        csv_file = StringIO()

        # Определяем заголовки и порядок полей
        fieldnames = [
            "ID", "Telegram ID", "Username", "Phone Number",
            "Registration Time (MSK)", "Reminder Sent"
        ]

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        # Заполняем данные
        for record in participants:
            writer.writerow({
                "ID": record["id"],
                "Telegram ID": record["telegram_user_id"],
                "Username": f"@{record['username']}" if record["username"] else "N/A",
                "Phone Number": record["phone_number"],
                "Registration Time (MSK)": record["registration_time"].strftime("%Y-%m-%d %H:%M:%S"),
                "Reminder Sent": "✓" if record["reminder_sent"] else "✗"
            })

        # Формируем имя файла с текущей датой
        filename = f"participants_{datetime.now().strftime('%Y-%m-%d')}.csv"

        # Отправляем файл администратору
        await message.answer_document(
            types.BufferedInputFile(
                csv_file.getvalue().encode('utf-8-sig'),  # Для корректного отображения кириллицы в Excel
                filename=filename
            ),
            caption=MESSAGES["admin"]["export_success"]
        )

        # Логируем действие
        logging.info(f"Admin {message.from_user.id} exported participants list")

    except Exception as e:
        logging.error(f"Export error: {e}", exc_info=True)
        await message.answer("⚠️ Произошла ошибка при формировании отчета")
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


# Новые команды администратора
@dp.message(Command("db_data"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_db_data(message: types.Message):
    conn = None
    try:
        conn = await get_db()
        participants = await conn.fetch("SELECT * FROM participants LIMIT 50")

        if not participants:
            await message.answer(MESSAGES["admin"]["db_empty"])
            return

        data = "\n".join(
            f"{p['id']}: @{p['username'] or 'N/A'} | {p['phone_number']} | {p['registration_time']}"
            for p in participants
        )
        await message.answer(MESSAGES["admin"]["db_data"].format(data=data))

    except Exception as e:
        logging.error(f"Ошибка при получении данных: {e}")
        await message.answer("Ошибка при получении данных")
    finally:
        if conn:
            await conn.close()


@dp.message(Command("db_empty"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_db_empty(message: types.Message):
    conn = None
    try:
        conn = await get_db()
        count = await conn.fetchval("SELECT COUNT(*) FROM participants")
        await message.answer(MESSAGES["admin"]["db_empty"] if count == 0 else "База данных не пуста")
    except Exception as e:
        logging.error(f"Ошибка проверки БД: {e}")
        await message.answer("Ошибка проверки БД")
    finally:
        if conn:
            await conn.close()


# Очистка базы данных с подтверждением
@dp.message(Command("clear_db"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_clear_db(message: types.Message):
    # Запрашиваем подтверждение
    confirm_clear[message.from_user.id] = True
    await message.answer(
        "⚠️ Вы уверены, что хотите полностью очистить базу данных?\n"
        "Это действие нельзя отменить!\n\n"
        "Отправьте 'ДА' для подтверждения или 'НЕТ' для отмены."
    )


@dp.message(F.text.in_({"ДА", "НЕТ"}) & F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def handle_clear_confirmation(message: types.Message):
    user_id = message.from_user.id
    if user_id not in confirm_clear:
        return

    if message.text == "НЕТ":
        del confirm_clear[user_id]
        await message.answer("Очистка базы данных отменена")
        return

    # Если подтвердили "ДА"
    conn = None
    try:
        conn = await get_db()
        count = await conn.fetchval("SELECT COUNT(*) FROM participants")

        if count == 0:
            await message.answer("База данных уже пуста")
            return

        await conn.execute("TRUNCATE TABLE participants RESTART IDENTITY CASCADE")
        await message.answer(f"✅ База данных очищена. Удалено {count} записей.")

    except Exception as e:
        logging.error(f"Ошибка очистки БД: {e}")
        await message.answer("⚠️ Произошла ошибка при очистке базы данных")
    finally:
        if conn:
            await conn.close()
        confirm_clear.pop(user_id, None)

# Удаление конкретного участника из базы данных
@dp.message(Command("delete_user"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_delete_user(message: types.Message):
    try:
        # Ожидаем команду в формате: /delete_user <user_id>
        user_id = int(message.text.split()[1])

        conn = await get_db()
        result = await conn.execute(
            "DELETE FROM participants WHERE telegram_user_id = $1",
            user_id
        )

        if result == "DELETE 1":
            await message.answer(f"✅ Пользователь {user_id} удален")
        else:
            await message.answer("Пользователь не найден")

    except (IndexError, ValueError):
        await message.answer("Использование: /delete_user <user_id>")
    except Exception as e:
        logging.error(f"Ошибка удаления пользователя: {e}")
        await message.answer("⚠️ Произошла ошибка при удалении пользователя")
    finally:
        if 'conn' in locals():
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