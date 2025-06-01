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
from aiogram.types import InputFile
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler

temp_storage = {}  # Глобальное временное хранилище для данных регистрации
confirm_clear = {} # Глобальная переменная для хранения состояния подтверждения удаления данных
current_user_to_delete = {}

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

# Обработчик первого входа в бота
@dp.message(F.chat_join_request | F.new_chat_members)
async def welcome_video(message: types.Message):
    try:
        # Проверяем, что пользователь новый
        if message.new_chat_members and message.new_chat_members[0].id == message.from_user.id:
            # Отправляем видео
            video = InputFile("media/welcome_video.mp4")

            # Кнопка регистрации
            builder = ReplyKeyboardBuilder()
            builder.add(KeyboardButton(text="🟢 Зарегистрироваться", request_contact=True))

            await message.answer_video(
                video=video,
                caption="Добро пожаловать на мероприятие «Бег, Кофе, Танцы»!\n\n"
                        "Нажмите кнопку ниже, чтобы зарегистрироваться:",
                reply_markup=builder.as_markup(
                    resize_keyboard=True,
                    one_time_keyboard=True
                )
            )

            # Логируем отправку
            logging.info(f"Sent welcome video to {message.from_user.id}")

    except Exception as e:
        logging.error(f"Error sending welcome video: {e}")
        await message.answer("Добро пожаловать! Нажмите /start для регистрации")

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
        temp_storage[message.from_user.id] = {
            "phone": phone,
            "timestamp": datetime.now()
        }
        await message.answer(
            "Пожалуйста, введите ваш @username вручную (без символа @):",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return

    # Сохраняем данные
    try:
        await save_participant(message.from_user.id, username, phone)
        await message.answer(
            MESSAGES["registration_success"],
            reply_markup=types.ReplyKeyboardRemove()
        )
    except Exception as e:
        logging.error(f"Ошибка регистрации: {e}")
        await message.answer("Произошла ошибка при регистрации. Попробуйте еще раз.")


# Обработка ручного ввода username
@dp.message(F.text & ~F.text.startswith('/'))
async def handle_username_input(message: types.Message):
    user_id = message.from_user.id

    # Проверяем, что пользователь начал процесс регистрации
    if user_id not in temp_storage:
        return

    # Проверяем таймаут (5 минут на ввод)
    if (datetime.now() - temp_storage[user_id]["timestamp"]) > timedelta(minutes=5):
        del temp_storage[user_id]
        await message.answer("Время ввода истекло. Пожалуйста, начните регистрацию заново.")
        return

    username = message.text.strip().strip('@')  # Удаляем пробелы и @

    # Валидация username
    if not (3 <= len(username) <= 32 and username.replace('_', '').isalnum()):
        await message.answer("Некорректный username. Должен содержать только буквы, цифры и _, длиной 3-32 символа.")
        return

    # Получаем номер из временного хранилища
    phone = temp_storage[user_id]["phone"]

    try:
        await save_participant(user_id, username, phone)
        await message.answer(MESSAGES["registration_success"])
    except asyncpg.exceptions.UniqueViolationError:
        await message.answer("Этот username уже занят. Пожалуйста, введите другой.")
    except Exception as e:
        logging.error(f"Ошибка сохранения: {e}")
        await message.answer("Произошла ошибка при сохранении данных. Попробуйте еще раз.")
    finally:
        # Удаляем данные из временного хранилища в любом случае
        temp_storage.pop(user_id, None)


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

# Команда удаления пользователя
@dp.message(Command("delete_user"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_delete_user(message: types.Message):
    try:
        # Получаем user_id из сообщения
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Использование: /delete_user <user_id>")
            return

        user_id = int(parts[1])
        current_user_to_delete[message.from_user.id] = user_id

        # Запрашиваем подтверждение
        await message.answer(
            f"⚠️ Вы уверены, что хотите удалить пользователя с ID {user_id}?\n"
            "Отправьте 'ДА' для подтверждения или 'НЕТ' для отмены."
        )
    except ValueError:
        await message.answer("Ошибка: user_id должен быть числом")


# Обработка подтверждения для удаления пользователя и очистки БД
@dp.message(F.text.in_(["ДА", "НЕТ"]), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def handle_confirmation(message: types.Message):
    user_id = message.from_user.id

    if user_id in confirm_clear:
        # Обработка подтверждения для очистки БД
        if message.text == "ДА":
            conn = None
            try:
                conn = await get_db()
                count = await conn.fetchval("SELECT COUNT(*) FROM participants")

                if count == 0:
                    await message.answer("База данных уже пуста")
                    return

                await conn.execute("TRUNCATE TABLE participants RESTART IDENTITY")
                await message.answer(f"✅ База данных очищена. Удалено {count} записей.")
            except Exception as e:
                logging.error(f"Ошибка очистки БД: {e}")
                await message.answer("⚠️ Произошла ошибка при очистке базы данных")
            finally:
                if conn:
                    await conn.close()
        else:
            await message.answer("Очистка базы данных отменена")

        confirm_clear.pop(user_id, None)

    elif user_id in current_user_to_delete:
        # Обработка подтверждения для удаления пользователя
        if message.text == "ДА":
            conn = None
            try:
                user_id_to_delete = current_user_to_delete[user_id]
                conn = await get_db()
                result = await conn.execute(
                    "DELETE FROM participants WHERE id = $1",
                    user_id_to_delete
                )

                if "DELETE 1" in result:
                    await message.answer(f"✅ Пользователь {user_id_to_delete} удален")
                else:
                    await message.answer("❌ Пользователь не найден")
            except Exception as e:
                logging.error(f"Ошибка удаления пользователя: {e}")
                await message.answer("⚠️ Произошла ошибка при удалении пользователя")
            finally:
                if conn:
                    await conn.close()
        else:
            await message.answer("Удаление пользователя отменено")

        current_user_to_delete.pop(user_id, None)


# Команда очистки БД с подтверждением
@dp.message(Command("clear_db"), F.from_user.id.in_(CONFIG["ADMIN_IDS"]))
async def cmd_clear_db(message: types.Message):
    confirm_clear[message.from_user.id] = True
    await message.answer(
        "⚠️ Вы уверены, что хотите полностью очистить базу данных?\n"
        "Это действие нельзя отменить!\n\n"
        "Отправьте 'ДА' для подтверждения или 'НЕТ' для отмены."
    )

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