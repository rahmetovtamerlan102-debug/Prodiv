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
MAX_CONCURRENT_REQUESTS = 2

if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
    raise Exception("Задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ---------- ГЛОБАЛЬНЫЕ СТРУКТУРЫ ----------
pending_requests: Dict[str, Tuple[int, asyncio.Future, float]] = {}
bot_entities_cache: Dict[str, object] = {}
trusted_bot_ids: Set[int] = set()
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# ---------- ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ----------
@user_client.on(events.NewMessage(incoming=True))
async def global_handler(event):
    if not event.is_private:
        return
    if not event.sender or not event.sender.bot:
        return
    sender_id = event.sender_id
    if sender_id not in trusted_bot_ids:
        return

    for req_id, (bot_id, future, _) in list(pending_requests.items()):
        if bot_id == sender_id and not future.done():
            future.set_result(event.message.text or event.raw_text or "")
            pending_requests.pop(req_id, None)
            logger.debug(f"Ответ для бота {sender_id} по req_id {req_id}")
            return

# ---------- ПОЛУЧЕНИЕ БОТА ----------
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

# ---------- УНИВЕРСАЛЬНЫЙ ЗАПРОС ----------
async def ask_bot(bot_username: str, query: str, timeout: int = TIMEOUT) -> Optional[str]:
    entity = await get_bot_entity(bot_username)
    if not entity:
        return None

    bot_id = entity.id
    req_id = str(uuid.uuid4())
    future = asyncio.get_event_loop().create_future()
    pending_requests[req_id] = (bot_id, future, datetime.now().timestamp())

    try:
        await user_client.send_message(entity, query)
        result = await asyncio.wait_for(future, timeout)
        return result
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds} сек для {bot_username}")
        await asyncio.sleep(e.seconds + 1)
        try:
            future = asyncio.get_event_loop().create_future()
            pending_requests[req_id] = (bot_id, future, datetime.now().timestamp())
            await user_client.send_message(entity, query)
            result = await asyncio.wait_for(future, timeout)
            return result
        except Exception as e2:
            logger.error(f"Повторная ошибка {bot_username}: {e2}")
            return None
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут {bot_username}")
        return None
    except Exception as e:
        logger.error(f"Ошибка {bot_username}: {e}")
        return None
    finally:
        if req_id in pending_requests:
            _, fut, _ = pending_requests[req_id]
            if not fut.done():
                fut.cancel()
            pending_requests.pop(req_id, None)

# ---------- НОМЕР ----------
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

# ---------- DATEREG ----------
async def get_dates(username: str) -> Dict[str, Optional[str]]:
    resp = await ask_bot(BOT_DATEREG, username, timeout=55)
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
    except Exception as e:
        logger.error(f"Direct API error for {identifier}: {e}")
    return res

# ---------- MORAX ----------
async def get_morax_data(username: str) -> Optional[str]:
    return await ask_bot(BOT_MORAX, username, timeout=60)

# ---------- ОТЧЁТ ----------
def build_report(username: str, phone: Optional[str], vek: dict, dates: dict, direct: dict, morax: Optional[str]) -> str:
    lines = []
    lines.append(f"🔍 ОТЧЁТ | {username}")
    lines.append("═══════════════════════════════")
    lines.append("")

    if phone:
        lines.append(f"📞 Номер: {phone}\n")
    else:
        lines.append("📞 Номер: не найден\n")

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
            lines.append(f"👤 ФИО: {vek['full_name']}\n")
        if vek.get("address"):
            lines.append(f"🏠 Адрес: {vek['address']}\n")

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
    lines.append(f"🖼 Аватар: {'есть' if direct.get('has_avatar') else 'нет'}")
    if direct.get("dc_id"):
        lines.append(f"🌐 Дата-центр: DC{direct['dc_id']}")
    lines.append("")

    lines.append("🕒 АКТИВНОСТЬ И ДАТЫ")
    lines.append(f"🕒 Статус: {direct.get('status_text') or 'неизвестно'}")
    if dates.get("account_age"):
        lines.append(f"📅 Регистрация (оценка): {dates['account_age']}")
    if dates.get("first_seen"):
        lines.append(f"👀 Появление (оценка): {dates['first_seen']}")
    lines.append("")

    if morax:
        lines.append("📊 MORAX (РАСШИРЕННЫЙ ПРОФИЛЬ)")
        lines.append(morax.strip())
        lines.append("")

    if not phone and not vek.get("operator") and not direct.get("id"):
        lines.append("❌ Информация не найдена.")
    return "\n".join(lines)

# ---------- ОБРАБОТЧИКИ ----------
@dp.message(Command('start'))
async def cmd_start(message: types.Message):
    await message.reply(
        "🔮 OSINT-бот v5\n\n"
        "Отправь юзернейм (например, @durov).\n"
        "Собираю:\n"
        "• 📞 номер телефона\n"
        "• 📡 данные из утечек\n"
        "• 🆔 точные данные из Telegram API\n"
        "• 📊 расширенный профиль из Morax\n\n"
        "⏳ Ожидание до 2 минут.",
        parse_mode="HTML"
    )

@dp.message()
async def handle_username(message: types.Message):
    if not message.text:
        return
    target = message.text.strip()
    if len(target) < 3:
        await message.reply("❌ Слишком короткий юзернейм.")
        return
    status = await message.reply(f"🔎 Сбор данных для {target}... (до 2 минут)")

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
    logger.info(f"✅ Telethon авторизован как @{me.username}")
    for name in [BOT_LIU, BOT_VEK, BOT_DATEREG, BOT_MORAX]:
        await get_bot_entity(name)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
