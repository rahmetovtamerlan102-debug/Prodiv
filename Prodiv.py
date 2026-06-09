#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import re
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, Set

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import UserStatusOnline, UserStatusOffline, UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth, UserStatusEmpty

# ---------- КОНФИГ ----------
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

BOT_LIU = "liuofxnhvm3dvqbot"
BOT_VEK = "vektokOsint_bot"
BOT_DATEREG = "dateregbot"
BOT_MORAX = "moraxgetbot"

TIMEOUT = 60
REQUEST_QUEUE_MAXSIZE = 10
MAX_CONCURRENT_REQUESTS = 2

if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
    raise Exception("Задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ---------- ГЛОБАЛЬНЫЕ СТРУКТУРЫ ----------
# pending_requests: req_id -> (bot_id, future, timestamp)
pending_requests: Dict[str, Tuple[int, asyncio.Future, float]] = {}
# Очередь для ботов (чтобы не слать параллельно)
bot_queues: Dict[str, asyncio.Queue] = {}
bot_entities_cache: Dict[str, object] = {}
trusted_bot_ids: Set[int] = set()
# Семафор для ограничения параллельных запросов к Telethon
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# ---------- ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОТВЕТОВ ----------
@user_client.on(events.NewMessage(incoming=True))
async def global_handler(event):
    if not event.is_private:
        return
    if not event.sender or not event.sender.bot:
        return
    sender_id = event.sender_id
    if sender_id not in trusted_bot_ids:
        return
    text = event.message.text or event.raw_text or ""
    if not text:
        return
    match = re.search(r'req_id:([a-f0-9\-]{36})', text)
    if not match:
        return
    req_id = match.group(1)
    if req_id not in pending_requests:
        return
    expected_bot_id, future, _ = pending_requests[req_id]
    if sender_id != expected_bot_id:
        return
    if future.done():
        return
    future.set_result(text)
    logger.debug(f"Ответ для {req_id} от бота {sender_id}")

# ---------- КЭШ БОТОВ ----------
async def get_bot_entity(bot_username: str):
    if bot_username in bot_entities_cache:
        return bot_entities_cache[bot_username]
    try:
        entity = await user_client.get_entity(bot_username)
        bot_entities_cache[bot_username] = entity
        trusted_bot_ids.add(entity.id)
        return entity
    except Exception as e:
        logger.error(f"Не найден {bot_username}: {e}")
        return None

# ---------- ОЧЕРЕДЬ ЗАПРОСОВ К БОТУ ----------
async def execute_request(entity, query: str, timeout: int = TIMEOUT) -> Optional[str]:
    bot_id = entity.id
    req_id = str(uuid.uuid4())
    future = asyncio.get_event_loop().create_future()
    pending_requests[req_id] = (bot_id, future, datetime.now().timestamp())
    
    full_query = f"{query}\n\nreq_id:{req_id}"
    
    try:
        await user_client.send_message(entity, full_query)
        result = await asyncio.wait_for(future, timeout)
        return result
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds} сек, ждём")
        await asyncio.sleep(e.seconds + 1)
        # Повторяем один раз
        try:
            future = asyncio.get_event_loop().create_future()
            pending_requests[req_id] = (bot_id, future, datetime.now().timestamp())
            await user_client.send_message(entity, full_query)
            result = await asyncio.wait_for(future, timeout)
            return result
        except Exception as e2:
            logger.error(f"Повторная ошибка: {e2}")
            return None
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут {timeout} сек")
        return None
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return None
    finally:
        if req_id in pending_requests:
            _, fut, _ = pending_requests[req_id]
            if not fut.done():
                fut.cancel()
            pending_requests.pop(req_id, None)

async def ask_bot(bot_username: str, query: str, timeout: int = TIMEOUT) -> Optional[str]:
    entity = await get_bot_entity(bot_username)
    if not entity:
        return None
    
    # Очередь для конкретного бота
    if bot_username not in bot_queues:
        bot_queues[bot_username] = asyncio.Queue(maxsize=REQUEST_QUEUE_MAXSIZE)
    queue = bot_queues[bot_username]
    
    async with global_semaphore:
        await queue.put(True)
        try:
            return await execute_request(entity, query, timeout)
        finally:
            await queue.get()

# ---------- НАЖАТИЕ КНОПКИ MORAX ----------
async def press_morax_button(entity, button_text: str, timeout: int = 45) -> Optional[str]:
    try:
        async for msg in user_client.iter_messages(entity, limit=1):
            if not msg.reply_markup:
                continue
            for row in msg.reply_markup.rows:
                for btn in row.buttons:
                    if button_text.lower() in btn.text.lower():
                        logger.info(f"Нажимаем кнопку: {btn.text}")
                        await msg.click(btn)
                        # Ждём ответ (пустой запрос, но с req_id)
                        return await ask_bot(BOT_MORAX, "", timeout=timeout)
        return None
    except Exception as e:
        logger.error(f"Ошибка нажатия {button_text}: {e}")
        return None

# ---------- НОМЕР ТЕЛЕФОНА ----------
def normalize_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r'[^\d+]', '', raw)
    m = re.search(r'(\+?\d{9,15})', digits)
    if not m:
        return None
    phone = m.group(1)
    if not phone.startswith('+') and len(phone) >= 10:
        phone = '+' + phone
    return phone

async def get_phone(username: str) -> Optional[str]:
    resp = await ask_bot(BOT_LIU, username, timeout=55)
    if resp:
        logger.info(f"[LIU] Ответ: {resp[:500]}")
        return normalize_phone(resp)
    return None

# ---------- VEKTOK ----------
async def get_vektok_data(phone: str) -> Dict[str, str]:
    result = {"operator": "", "region": "", "full_name": "", "bases_count": "", "records_count": "", "address": ""}
    if not phone:
        return result
    resp = await ask_bot(BOT_VEK, phone, timeout=65)
    if resp:
        parsed = parse_vektok_text(resp)
        result.update(parsed)
    return result

def parse_vektok_text(text: str) -> Dict[str, str]:
    res = {}
    patterns = {
        "operator": r'Оператор:\s*([^\n]+)',
        "region": r'Регион:\s*([^\n]+)',
        "full_name": r'(?:Найденные данные:|ЗАПИСАН В БАЗАХ:)\s*([^\n]+)',
        "bases_count": r'Количество баз:\s*(\d+)',
        "records_count": r'Количество записей:\s*(\d+)',
        "address": r'Адрес:\s*([^\n]+)'
    }
    for k, p in patterns.items():
        m = re.search(p, text, re.I | re.M)
        if m:
            res[k] = m.group(1).strip()
    return res

# ---------- DATEREGBOT ----------
async def get_dates(username: str) -> Dict[str, Optional[str]]:
    resp = await ask_bot(BOT_DATEREG, username, timeout=55)
    result = {"first_seen": None, "account_age": None}
    if resp:
        m = re.search(r'(?:Первое появление|First seen):\s*([^\n]+)', resp, re.I)
        if m:
            result["first_seen"] = re.sub(r'\s*\([^)]*\)', '', m.group(1)).strip()
        m = re.search(r'(?:Регистрация|Account created|Registration):\s*([^\n]+)', resp, re.I)
        if m:
            result["account_age"] = re.sub(r'\s*\([^)]*\)', '', m.group(1)).strip()
    return result

# ---------- ПРЯМОЙ API TELEGRAM ----------
def get_status_text(status) -> str:
    if isinstance(status, UserStatusOnline):
        return "онлайн"
    if isinstance(status, UserStatusOffline) and status.was_online:
        was = status.was_online
        if isinstance(was, datetime):
            if was.tzinfo is None:
                was = was.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            days = (now - was).days
            if days == 0:
                return "сегодня"
            if days < 7:
                return "на этой неделе"
            return "давно"
        return "офлайн"
    if isinstance(status, UserStatusRecently):
        return "недавно"
    if isinstance(status, UserStatusLastWeek):
        return "на прошлой неделе"
    if isinstance(status, UserStatusLastMonth):
        return "в прошлом месяце"
    if isinstance(status, UserStatusEmpty):
        return "скрыто"
    return "неизвестно"

async def get_direct_info(identifier: str) -> Dict[str, Any]:
    res = {
        "id": None, "first_name": None, "last_name": None, "username": None,
        "usernames": [], "bio": None, "premium": False, "verified": False,
        "scam": False, "fake": False, "bot": False, "status_text": None,
        "has_avatar": False, "dc_id": None
    }
    try:
        clean_id = identifier.lstrip('@')
        entity = await user_client.get_entity(clean_id)
        if getattr(entity, 'bot', False):
            res["bot"] = True
            return res
        full = await user_client(GetFullUserRequest(entity.id))
        user = entity
        res["id"] = user.id
        res["first_name"] = user.first_name
        res["last_name"] = user.last_name
        res["username"] = f"@{user.username}" if user.username else None
        if hasattr(user, 'usernames') and user.usernames:
            res["usernames"] = [f"@{u.username}" for u in user.usernames]
        res["bio"] = full.about or getattr(user, "about", None)
        res["premium"] = getattr(user, 'premium', False)
        res["verified"] = getattr(user, 'verified', False)
        res["scam"] = getattr(user, 'scam', False)
        res["fake"] = getattr(user, 'fake', False)
        res["bot"] = getattr(user, 'bot', False)
        res["status_text"] = get_status_text(user.status)
        if hasattr(user, 'photo') and user.photo:
            res["has_avatar"] = True
            if hasattr(user.photo, 'dc_id'):
                res["dc_id"] = user.photo.dc_id
    except Exception as e:
        logger.error(f"Direct API error: {e}")
    return res

# ---------- MORAX (ПОДАРКИ + ЧАСТОТА СЛОВ) ----------
async def get_morax_data(username: str) -> Dict[str, Optional[str]]:
    result = {"profile": None, "gifts": None, "words": None}
    entity = await get_bot_entity(BOT_MORAX)
    if not entity:
        return result
    
    # Профиль
    result["profile"] = await ask_bot(BOT_MORAX, username, timeout=60)
    await asyncio.sleep(2)
    
    # Подарки
    gifts = await press_morax_button(entity, "Подарки")
    result["gifts"] = gifts
    await asyncio.sleep(2)
    
    # Частота слов
    words = await press_morax_button(entity, "Частота слов")
    if not words:
        words = await press_morax_button(entity, "Частота сл")
    result["words"] = words
    return result

# ---------- ОТЧЁТ ----------
def build_report(username: str, phone: Optional[str], vek: dict, dates: dict, direct: dict, morax: dict) -> str:
    lines = []
    lines.append(f"🔍 ОТЧЁТ | @{username}")
    lines.append("═══════════════════════════════")
    lines.append("")
    if phone:
        lines.append(f"📞 Номер: {phone}\n")
    else:
        lines.append("📞 Номер: не найден\n")
    if vek.get("operator"):
        lines.append(f"📡 Оператор: {vek['operator']}")
    if vek.get("region"):
        lines.append(f"🌍 Регион: {vek['region']}")
    if vek.get("full_name"):
        lines.append(f"👤 ФИО: {vek['full_name']}\n")
    if vek.get("address"):
        lines.append(f"🏠 Адрес: {vek['address']}\n")
    lines.append("🆔 TELEGRAM")
    lines.append(f"ID: {direct.get('id') or 'неизвестно'}")
    if direct.get("usernames"):
        lines.append(f"Username: {', '.join(direct['usernames'])}")
    elif direct.get("username"):
        lines.append(f"Username: {direct['username']}")
    lines.append(f"Имя: {direct.get('first_name') or '—'} {direct.get('last_name') or ''}")
    lines.append(f"Био: {direct.get('bio') or '—'}")
    lines.append(f"Премиум: {'✅' if direct.get('premium') else '❌'}")
    lines.append(f"Верифицирован: {'✅' if direct.get('verified') else '❌'}")
    lines.append(f"Бот: {'✅' if direct.get('bot') else '❌'}")
    lines.append(f"Аватар: {'есть' if direct.get('has_avatar') else 'нет'}")
    if direct.get("dc_id"):
        lines.append(f"Дата-центр: DC{direct['dc_id']}")
    lines.append("")
    lines.append("🕒 АКТИВНОСТЬ")
    lines.append(f"Статус: {direct.get('status_text') or 'неизвестно'}")
    if dates.get("first_seen"):
        lines.append(f"Первое появление (оценка): {dates['first_seen']}")
    if dates.get("account_age"):
        lines.append(f"Возраст аккаунта (оценка): {dates['account_age']}")
    lines.append("")
    if morax.get("profile"):
        lines.append("📊 MORAX (профиль)\n" + morax["profile"][:3000] + "\n")
    if morax.get("gifts"):
        lines.append("🎁 MORAX (подарки)\n" + morax["gifts"][:2000] + "\n")
    if morax.get("words"):
        lines.append("🗣️ MORAX (частота слов)\n" + morax["words"][:2000] + "\n")
    if not phone and not vek.get("operator") and not direct.get("id"):
        lines.append("❌ Информация не найдена.")
    return "\n".join(lines)

# ---------- ОБРАБОТЧИКИ ----------
@dp.message(Command('start'))
async def cmd_start(message: types.Message):
    await message.reply(
        "🔮 OSINT-бот v4 (прод-ядро)\n"
        "Отправь юзернейм (например, @durov).\n"
        "Собираю: номер, данные из утечек, активность, Morax (профиль/подарки/частота слов).\n"
        "⏳ Ожидание до 3 минут.",
        parse_mode="HTML"
    )

@dp.message()
async def handle_username(message: types.Message):
    if not message.text:
        return
    target = message.text.strip().lstrip('@')
    if len(target) < 3:
        await message.reply("❌ Слишком короткий юзернейм.")
        return
    status = await message.reply(f"🔎 Сбор данных для @{target}...")
    phone = await get_phone(target)
    vek = await get_vektok_data(phone) if phone else {}
    dates = await get_dates(target)
    direct = await get_direct_info(target)
    morax = await get_morax_data(target)
    report = build_report(target, phone, vek, dates, direct, morax)
    await status.edit_text(report, parse_mode="HTML", disable_web_page_preview=True)

# ---------- ЗАПУСК ----------
async def main():
    await bot.delete_webhook()
    if not user_client.is_connected():
        await user_client.start()
    me = await user_client.get_me()
    logger.info(f"✅ Telethon как @{me.username}")
    for name in [BOT_LIU, BOT_VEK, BOT_DATEREG, BOT_MORAX]:
        await get_bot_entity(name)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
