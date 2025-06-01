import asyncio
import csv
import io
import json
import logging
from datetime import datetime, timedelta
from aiogram.client.default import DefaultBotProperties
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from pathlib import Path
from aiogram.enums import ParseMode
from aiogram.types import FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from asyncpg import create_pool

# === Загрузка конфигурации и сообщений ===
with open("config.json", encoding="utf-8") as f:
    CONFIG = json.load(f)

with open("messages.json", encoding="utf-8") as f:
    MESSAGES = json.load(f)

TOKEN = CONFIG["BOT_TOKEN"]
EVENT_DATE = datetime.strptime(CONFIG["EVENT_DATE"], "%Y-%m-%d")
ADMIN_IDS = CONFIG["ADMIN_IDS"]

# === Инициализация бота ===
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
db_pool = None

# === Логирование ===
logging.basicConfig(level=logging.INFO)

# === Состояния FSM ===
class Registration(StatesGroup):
    waiting_for_contact = State()
    waiting_for_username = State()
    waiting_for_broadcast_text = State()
    waiting_for_delete_id = State()

# === Клавиатура для администратора ===
admin_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Список участников"), KeyboardButton(text="📁 Экспорт CSV")],
        [KeyboardButton(text="📣 Рассылка"), KeyboardButton(text="❌ Удалить участника")]
    ],
    resize_keyboard=True
)

# === Фильтр: проверка ID администратора ===
def is_admin(message: types.Message) -> bool:
    return message.from_user.id in ADMIN_IDS

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    video_path = Path("media") / "welcome_video.mp4"
    if video_path.exists():
        try:
            video = FSInputFile(path=video_path)
            await message.answer_video(video=video, caption="👋 Добро пожаловать! Ниже — информация о мероприятии.")
        except Exception as e:
            logging.error(f"Ошибка отправки видео: {e}")
            await message.answer("👋 Добро пожаловать! (не удалось отправить видео)")
    else:
        logging.warning("Файл welcome_video.mp4 не найден в папке media.")
        await message.answer("👋 Добро пожаловать! (видео отсутствует)")
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True
    )
    await message.answer(MESSAGES["start"], reply_markup=kb)
    await state.set_state(Registration.waiting_for_contact)

@dp.message(Registration.waiting_for_contact, F.contact)
async def receive_contact(message: types.Message, state: FSMContext):
    phone = message.contact.phone_number
    username = message.from_user.username
    if username:
        await save_user(message.from_user.id, username, phone)
        await message.answer(MESSAGES["registration_success"], reply_markup=types.ReplyKeyboardRemove())
        await state.clear()
    else:
        await state.update_data(phone=phone)
        await message.answer("👤 Введите ваш @username вручную")
        await state.set_state(Registration.waiting_for_username)

@dp.message(Registration.waiting_for_username)
async def receive_username(message: types.Message, state: FSMContext):
    data = await state.get_data()
    phone = data["phone"]
    await save_user(message.from_user.id, message.text.lstrip("@"), phone)
    await message.answer(MESSAGES["registration_success"])
    await state.clear()

async def save_user(user_id, username, phone):
    async with db_pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO participants (telegram_user_id, username, phone_number, registration_time, reminder_sent)
        VALUES ($1, $2, $3, NOW(), FALSE)
        ON CONFLICT (telegram_user_id) DO NOTHING;
        """, user_id, username, phone)

@dp.message(lambda m: m.text == "📊 Список участников" and is_admin(m))
async def list_participants(message: types.Message):
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM participants;")
        await message.answer(MESSAGES["admin"]["list"].format(count))

@dp.message(lambda m: m.text == "📁 Экспорт CSV" and is_admin(m))
async def export_csv(message: types.Message):
    import zipfile
    import tempfile

    async with db_pool.acquire() as conn:
        records = await conn.fetch("SELECT * FROM participants;")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Telegram ID", "Username", "Phone", "Time", "Reminder"])
    for r in records:
        writer.writerow(r.values())
    file_bytes = output.getvalue().encode("utf-8-sig")
    today = datetime.now().strftime("%Y-%m-%d")
    csv_filename = f"participants_{today}.csv"
    zip_filename = f"export_{today}.zip"

    # Создаем временный zip-файл
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / zip_filename
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(csv_filename, file_bytes)

        with zip_path.open("rb") as f:
            archive = types.BufferedInputFile(f.read(), filename=zip_filename)
            await message.answer_document(document=archive)

@dp.message(lambda m: m.text == "📣 Рассылка" and is_admin(m))
async def ask_broadcast_text(message: types.Message, state: FSMContext):
    await message.answer("Введите текст для рассылки:")
    await state.set_state(Registration.waiting_for_broadcast_text)

@dp.message(Registration.waiting_for_broadcast_text, lambda m: is_admin(m))
async def broadcast_text(message: types.Message, state: FSMContext):
    text = message.text.strip()
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT telegram_user_id FROM participants;")
    success = 0
    for u in users:
        try:
            await bot.send_message(u["telegram_user_id"], text)
            success += 1
        except:
            continue
    await message.answer(MESSAGES["admin"]["broadcast_success"].format(success), reply_markup=admin_kb)
    await state.clear()

@dp.message(lambda m: m.text == "❌ Удалить участника" and is_admin(m))
async def ask_user_id(message: types.Message, state: FSMContext):
    await message.answer("Введите ID участника для удаления:")
    await state.set_state(Registration.waiting_for_delete_id)

@dp.message(Registration.waiting_for_delete_id, lambda m: is_admin(m))
async def delete_user(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
        async with db_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM participants WHERE id = $1", user_id)
            if result == "DELETE 1":
                await message.answer(MESSAGES["admin"]["delete_user"].format(user_id=user_id), reply_markup=admin_kb)
            else:
                await message.answer("❌ Участник с таким ID не найден.")
    except Exception as e:
        logging.error(f"Ошибка при удалении участника: {e}")
        await message.answer("Ошибка при удалении участника")
    await state.clear()

@dp.message(Command("admin"))
async def admin_menu(message: types.Message):
    if is_admin(message):
        await message.answer("Панель администратора:", reply_markup=admin_kb)

async def send_reminders():
    async with db_pool.acquire() as conn:
        users = await conn.fetch("""
        SELECT id, telegram_user_id FROM participants
        WHERE reminder_sent = FALSE
        AND NOW() + interval '24 hour' >= $1;
        """, EVENT_DATE)
        for u in users:
            await bot.send_message(u["telegram_user_id"], MESSAGES["reminder"])
            await conn.execute("UPDATE participants SET reminder_sent = TRUE WHERE id = $1", u["id"])

async def main():
    global db_pool
    dsn = f"postgresql://{CONFIG['DB_USER']}:{CONFIG['DB_PASSWORD']}@{CONFIG['DB_HOST']}/{CONFIG['DB_NAME']}"
    db_pool = await create_pool(dsn=dsn)
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_reminders, "interval", minutes=60)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
