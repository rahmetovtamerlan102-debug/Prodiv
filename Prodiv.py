#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import re
import logging
import signal
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler

from cachetools import TTLCache
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    UserStatusOnline, UserStatusOffline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth, UserStatusEmpty
)

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
MORAX_TIMEOUT = 90
MAX_CONCURRENT_USERS = 2
CACHE_TTL = 3600
CACHE_MAXSIZE = 1000

if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
    raise Exception("Задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")

# ---------- ЛОГИ ----------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = RotatingFileHandler('bot.log', maxBytes=1_000_000, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console)

# ---------- КЛИЕНТЫ ----------
user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ---------- ГЛОБАЛЬНЫЕ СТРУКТУРЫ ----------
cache = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL)
cache_lock = asyncio.Lock()
user_locks: Dict[str, asyncio.Lock] = {}
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_USERS)

async def get_user_lock(username: str) -> asyncio.Lock:
    if username not in user_locks:
        user_locks[username] = asyncio.Lock()
    return user_locks[username]

# ---------- УНИВЕРСАЛЬНЫЙ ЗАПРОС К БОТУ (СИНХРОННЫЙ HANDLER) ----------
async def ask_bot_no_reqid(bot_username: str, query: str, timeout: int = TIMEOUT) -> Optional[str]:
    try:
        entity = await user_client.get_entity(bot_username)
    except Exception as e:
        logger.error(f"Не найден бот {bot_username}: {e}")
        return None

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def handler(event):
        if future.done():
            return
        text = event.message.text or event.raw_text or ""
        if text:
            loop.call_soon_threadsafe(future.set_result, text)
            try:
                user_client.remove_event_handler(handler)
            except:
                pass

    user_client.add_event_handler(handler, events.NewMessage(from_users=entity.id))

    try:
        await user_client.send_message(entity, query)
        result = await asyncio.wait_for(future, timeout)
        return result
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds} сек для {bot_username}")
        await asyncio.sleep(e.seconds + 1)
        return await ask_bot_no_reqid(bot_username, query, timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут {bot_username}")
        return None
    except Exception as e:
        logger.error(f"Ошибка {bot_username}: {e}")
        return None
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass

# ---------- MORAX (С ПРОПУСКОМ ПРОМЕЖУТОЧНЫХ, СИНХРОННЫЙ HANDLER) ----------
async def ask_morax(bot_username: str, query: str, timeout: int = MORAX_TIMEOUT) -> Optional[str]:
    try:
        entity = await user_client.get_entity(bot_username)
    except Exception as e:
        logger.error(f"Не найден бот {bot_username}: {e}")
        return None

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def handler(event):
        if future.done():
            return
        text = event.message.text or event.raw_text or ""
        if not text:
            return
        if any(word in text.lower() for word in ["вычисляю", "загрузка", "подождите", "идет сбор"]):
            return
        if len(text) > 50 or "ID:" in text or "usernames:" in text:
            loop.call_soon_threadsafe(future.set_result, text)
            try:
                user_client.remove_event_handler(handler)
            except:
                pass

    user_client.add_event_handler(handler, events.NewMessage(from_users=entity.id))

    try:
        await user_client.send_message(entity, query)
        logger.info(f"Morax: запрос отправлен для {query}, ожидание до {timeout} сек")
        result = await asyncio.wait_for(future, timeout)
        logger.info(f"Morax: получен ответ длиной {len(result)}")
        return result
    except asyncio.TimeoutError:
        logger.warning(f"Morax таймаут {timeout} сек")
        return None
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass

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
    resp = await ask_bot_no_reqid(BOT_LIU, username, timeout=55)
    if resp:
        return normalize_phone(resp)
    return None

# ---------- VEKTOK ----------
async def get_vektok_data(phone: str) -> Dict[str, str]:
    result = {"operator": "", "region": "", "full_name": "", "bases_count": "", "records_count": "", "address": ""}
    if not phone:
        return result
    resp = await ask_bot_no_reqid(BOT_VEK, phone, timeout=65)
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

# ---------- DATEREG ----------
async def get_dates(username: str) -> Dict[str, Optional[str]]:
    resp = await ask_bot_no_reqid(BOT_DATEREG, username, timeout=55)
    result = {"first_seen": None, "account_age": None}
    if resp:
        m = re.search(r'(?:Первое появление|First seen):\s*([^\n]+)', resp, re.I)
        if m:
            result["first_seen"] = m.group(1).strip()
        m = re.search(r'(?:Регистрация|Account created|Registration):\s*([^\n]+)', resp, re.I)
        if m:
            result["account_age"] = m.group(1).strip()
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
        return "недавно (до месяца)"
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
        "has_avatar": False, "dc_id": None, "avatar_url": None
    }
    try:
        clean_id = identifier.lstrip('@')
        entity = await user_client.get_entity(clean_id)
        if getattr(entity, 'bot', False):
            res["bot"] = True
            return res
        full = await user_client(GetFullUserRequest(entity.id))
        user = entity
        bio = None
        if hasattr(full, 'about') and full.about:
            bio = full.about
        elif hasattr(full, 'full_user') and hasattr(full.full_user, 'about') and full.full_user.about:
            bio = full.full_user.about
        elif hasattr(user, 'about') and user.about:
            bio = user.about
        res["id"] = user.id
        res["first_name"] = user.first_name
        res["last_name"] = user.last_name
        res["username"] = f"@{user.username}" if user.username else None
        if hasattr(user, 'usernames') and user.usernames:
            res["usernames"] = [f"@{u.username}" for u in user.usernames]
        res["bio"] = bio
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
            if user.username:
                res["avatar_url"] = f"https://t.me/{user.username}"
    except Exception as e:
        logger.error(f"Direct API error for {identifier}: {e}")
    return res

# ---------- MORAX ----------
async def get_morax_data(username: str) -> Optional[str]:
    return await ask_morax(BOT_MORAX, username)

# ---------- ОСНОВНАЯ ФУНКЦИЯ СБОРА ----------
async def collect_all(username: str) -> Dict[str, Any]:
    async with cache_lock:
        if username in cache:
            logger.info(f"Кэш для {username}")
            return cache[username]

    phone_task = asyncio.create_task(get_phone(username))
    dates_task = asyncio.create_task(get_dates(username))
    direct_task = asyncio.create_task(get_direct_info(username))
    morax_task = asyncio.create_task(get_morax_data(username))

    phone = await phone_task
    if phone:
        vek_task = asyncio.create_task(get_vektok_data(phone))
    else:
        vek_task = asyncio.create_task(asyncio.sleep(0, result={}))

    dates, direct, morax, vek = await asyncio.gather(
        dates_task, direct_task, morax_task, vek_task
    )

    result = {
        "username": username,
        "phone": phone,
        "vektok": vek,
        "dates": dates,
        "direct": direct,
        "morax": morax
    }
    async with cache_lock:
        cache[username] = result
    return result

# ---------- ОТЧЁТ ----------
def clean_date(date_str: str) -> str:
    if not date_str:
        return date_str
    return re.sub(r'\s*\([^)]*\)', '', date_str).strip()

def build_report(data: Dict[str, Any]) -> str:
    username = data["username"]
    phone = data["phone"]
    vek = data["vektok"]
    dates = data["dates"]
    direct = data["direct"]
    morax = data["morax"]

    lines = []
    lines.append(f"🔍 ОТЧЁТ | {username}")
    lines.append("═══════════════════════════════════════════════════════════════")
    lines.append("")

    if phone:
        lines.append(f"📞 Номер: {phone}")
        lines.append("")
    else:
        lines.append("📞 Номер: не найден")
        lines.append("")

    if vek.get("operator") or vek.get("full_name"):
        lines.append("📡 ДАННЫЕ ИЗ УТЕЧЕК")
        if vek.get("operator"):
            lines.append(f"📡 Оператор: {vek['operator']}")
        if vek.get("region"):
            lines.append(f"🌍 Регион: {vek['region']}")
        if vek.get("bases_count"):
            lines.append(f"🗄️ Базы: {vek['bases_count']} записей")
        lines.append("")
        if vek.get("full_name"):
            lines.append(f"👤 ФИО: {vek['full_name']}")
            lines.append("")
        if vek.get("address"):
            lines.append(f"🏠 Адрес: {vek['address']}")
            lines.append("")

    lines.append("🆔 TELEGRAM API (ТОЧНЫЕ ДАННЫЕ)")
    lines.append(f"🆔 ID: {direct.get('id') or 'неизвестно'}")
    if direct.get("usernames"):
        lines.append(f"📛 Активные username: {', '.join(direct['usernames'])}")
    elif direct.get("username"):
        lines.append(f"📛 Юзернейм: {direct['username']}")
    lines.append(f"👤 Имя: {direct.get('first_name') or '—'} {direct.get('last_name') or ''}")
    lines.append(f"📝 Био: {direct.get('bio') or '—'}")
    lines.append(f"⭐ Премиум: {'✅' if direct.get('premium') else '❌'}")
    lines.append(f"✅ Верифицирован: {'✅' if direct.get('verified') else '❌'}")
    lines.append(f"⚠️ Scam/Fake: {'✅' if direct.get('scam') else '❌'} / {'✅' if direct.get('fake') else '❌'}")
    lines.append(f"🤖 Бот: {'✅' if direct.get('bot') else '❌'}")
    if direct.get("has_avatar"):
        lines.append(f"🖼 Аватар: есть")
        if direct.get("avatar_url"):
            lines.append(f"🔗 Ссылка: {direct['avatar_url']}")
    else:
        lines.append("🖼 Аватар: нет")
    if direct.get("dc_id"):
        lines.append(f"🌐 Дата-центр: DC{direct['dc_id']}")
    lines.append("")

    lines.append("🕒 АКТИВНОСТЬ И ДАТЫ")
    lines.append(f"🕒 Статус: {direct.get('status_text') or 'неизвестно'}")
    if dates.get("account_age"):
        lines.append(f"📅 Регистрация (оценка): {clean_date(dates['account_age'])}")
    if dates.get("first_seen"):
        lines.append(f"👀 Появление (оценка): {clean_date(dates['first_seen'])}")
    lines.append("")

    if morax:
        lines.append("📊 MORAX (РАСШИРЕННЫЙ ПРОФИЛЬ)")
        clean = morax.strip()
        clean = re.sub(r'^[ㅤᅠ\s]+$', '', clean, flags=re.MULTILINE)
        lines.append(clean)
        lines.append("")

    if not phone and not vek.get("operator") and not direct.get("id"):
        lines.append("❌ Информация не найдена.")
    return "\n".join(lines)

# ---------- ОБРАБОТЧИКИ ----------
@dp.message(Command('start'))
async def cmd_start(message: types.Message):
    await message.reply(
        "🔮 **OSINT-бот (финальная версия, без ошибок NoneType)**\n\n"
        "Отправь юзернейм (например, @durov).\n\n"
        "✅ 1 запрос к каждому боту\n"
        "✅ Параллельный сбор\n"
        "✅ Исправлены все ошибки Telethon\n"
        "✅ Graceful shutdown на Render\n"
        "⏳ Ожидание до 2 минут.",
        parse_mode="HTML"
    )

@dp.message()
async def handle_username(message: types.Message):
    if not message.text:
        return
    raw = message.text.strip()
    cleaned = raw.lstrip('@')
    if not cleaned:
        await message.reply("❌ Пустой юзернейм.")
        return
    target = '@' + cleaned
    if len(cleaned) < 3:
        await message.reply("❌ Слишком короткий юзернейм.")
        return

    user_lock = await get_user_lock(target)
    async with user_lock:
        async with global_semaphore:
            status = await message.reply(f"🔎 Сбор данных для {target}... (до 2 минут)")
            data = await collect_all(target)
            report = build_report(data)
            await status.edit_text(report, parse_mode="HTML", disable_web_page_preview=True)

# ---------- GRACEFUL SHUTDOWN ----------
async def shutdown(polling_task: asyncio.Task):
    logger.info("Получен сигнал завершения (SIGTERM), останавливаю бота...")
    polling_task.cancel()
    try:
        await polling_task
    except asyncio.CancelledError:
        pass
    await user_client.disconnect()
    await bot.session.close()
    logger.info("Бот остановлен корректно.")

# ---------- ЗАПУСК ----------
async def main():
    await bot.delete_webhook()
    if not user_client.is_connected():
        await user_client.start()
    me = await user_client.get_me()
    logger.info(f"✅ Telethon авторизован как @{me.username}")

    polling_task = asyncio.create_task(dp.start_polling(bot))

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(polling_task)))

    await polling_task

if __name__ == "__main__":
    asyncio.run(main())
