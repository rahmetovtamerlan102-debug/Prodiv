#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import re
import logging
import zipfile
import io
import json
import subprocess
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth, UserStatusEmpty

# ========== КОНФИГУРАЦИЯ ==========
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
BASE_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://prodiv.onrender.com")
WEBHOOK_PATH = "/webhook"
PORT = int(os.environ.get("PORT", 8080))
CACHE_TTL_SECONDS = 3600
MAX_CONCURRENT_USERS = 2
USER_RATE_LIMIT_SECONDS = 15
BOT_REQUEST_DELAY = 1.5   # пауза между запросами к разным ботам

if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
    raise Exception("❌ Задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== КЛИЕНТЫ ==========
user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== ГЛОБАЛЬНЫЕ ОГРАНИЧИТЕЛИ И КЭШ ==========
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_USERS)
user_locks: Dict[str, asyncio.Lock] = {}
user_last_request: Dict[int, datetime] = {}
cache: Dict[str, dict] = {}
aiohttp_session: Optional[aiohttp.ClientSession] = None

# ========== КЭШ ДЛЯ ID БОТОВ (чтобы не дёргать get_entity каждый раз) ==========
bot_ids_cache: Dict[str, int] = {}

async def get_bot_id(bot_username: str) -> Optional[int]:
    if bot_username in bot_ids_cache:
        return bot_ids_cache[bot_username]
    try:
        entity = await user_client.get_entity(bot_username)
        bot_ids_cache[bot_username] = entity.id
        return entity.id
    except Exception as e:
        logger.warning(f"Не удалось найти бота {bot_username}: {e}")
        return None

# ========== УНИВЕРСАЛЬНЫЙ ЗАПРОС К БОТУ (без req_id) ==========
async def ask_bot(bot_username: str, query: str, timeout: int = 15) -> Optional[str]:
    """Отправляет запрос боту, ждёт ответ (первое сообщение от этого бота)."""
    bot_id = await get_bot_id(bot_username)
    if not bot_id:
        return None

    # Создаём будущее и временный обработчик
    future = asyncio.get_event_loop().create_future()

    @user_client.on(events.NewMessage(from_users=bot_id, incoming=True))
    async def handler(event):
        # Получаем текст ответа
        text = event.message.text or ""
        # Если будущее ещё не завершено, завершаем его
        if not future.done():
            future.set_result(text)
        # Удаляем обработчик сразу после получения ответа
        user_client.remove_event_handler(handler)

    try:
        entity = await user_client.get_entity(bot_username)
        await user_client.send_message(entity, query)
        result = await asyncio.wait_for(future, timeout)
        return result
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут {bot_username}")
        user_client.remove_event_handler(handler)
        return None
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds} сек для {bot_username}")
        await asyncio.sleep(e.seconds + 0.5)
        user_client.remove_event_handler(handler)
        return await ask_bot(bot_username, query, timeout)
    except Exception as e:
        logger.error(f"Ошибка {bot_username}: {e}")
        user_client.remove_event_handler(handler)
        return None

# ========== 1. НОМЕР ТЕЛЕФОНА (через @liuofxnhvm3dvqbot) ==========
async def get_phone(username: str) -> Optional[str]:
    resp = await ask_bot("liuofxnhvm3dvqbot", username, timeout=20)
    if resp:
        m = re.search(r'(\+?7\d{10})', resp)
        if m:
            phone = m.group(1)
            if not phone.startswith('+'):
                phone = '+' + phone
            return phone
    return None

# ========== 2. ДАННЫЕ ОТ @vektokOsint_bot (оператор, регион, ФИО, адрес, базы) ==========
async def get_vektok_info(phone: str) -> Dict[str, Any]:
    result = {
        "operator": "", "region": "", "full_name": "",
        "bases_count": "", "records_count": "", "address": ""
    }
    bot_id = await get_bot_id("vektokOsint_bot")
    if not bot_id:
        return result

    future = asyncio.get_event_loop().create_future()

    @user_client.on(events.NewMessage(from_users=bot_id, incoming=True))
    async def doc_handler(event):
        if event.message.document:
            file_path = await event.message.download_media()
            with open(file_path, 'rb') as f:
                data = f.read()
            os.remove(file_path)
            if file_path.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for name in zf.namelist():
                        if name.endswith('.html'):
                            html = zf.read(name).decode('utf-8', errors='ignore')
                            break
            else:
                html = data.decode('utf-8', errors='ignore')
            parsed = parse_vektok_html(html)
            future.set_result(parsed)
            user_client.remove_event_handler(doc_handler)
        elif event.message.text and "не удалось" in event.message.text.lower():
            # Бот ответил текстом об ошибке – тоже завершаем
            future.set_result(None)
            user_client.remove_event_handler(doc_handler)

    try:
        entity = await user_client.get_entity("vektokOsint_bot")
        await user_client.send_message(entity, phone)
        parsed = await asyncio.wait_for(future, timeout=30)
        if parsed:
            result.update(parsed)
    except asyncio.TimeoutError:
        logger.warning("Таймаут vektokOsint_bot")
        user_client.remove_event_handler(doc_handler)
    except Exception as e:
        logger.error(f"Ошибка vektok: {e}")
        user_client.remove_event_handler(doc_handler)
    return result

def parse_vektok_html(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator='\n')
    res = {}
    patterns = {
        "operator": r'Оператор:\s*(.+?)(?:\n|$)',
        "region": r'Регион:\s*(.+?)(?:\n|$)',
        "full_name": r'(?:Найденные данные:|ЗАПИСАН В БАЗАХ:)\s*(.+?)(?:\n|$)',
        "bases_count": r'Количество баз:\s*(\d+)',
        "records_count": r'Количество записей:\s*(\d+)',
        "address": r'Адрес:\s*(.+?)(?:\n|$)'
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text, re.I | re.M)
        if m:
            res[key] = m.group(1).strip()
    return res

# ========== 3. ДАТА РЕГИСТРАЦИИ ОТ @dateregbot ==========
async def get_registration_date(username: str) -> Optional[str]:
    resp = await ask_bot("dateregbot", username, timeout=12)
    if resp:
        m = re.search(r'(\d{2}[./]\d{2}[./]\d{4})', resp)
        if m:
            return m.group(1)
        m = re.search(r'(\d{4}-\d{2}-\d{2})', resp)
        if m:
            return m.group(1)
    return None

# ========== 4. ИСТОРИЯ СМЕНЫ ИМЕНИ/ЮЗЕРНЕЙМА ОТ @SangMata_beta_bot ==========
async def get_name_history(username: str) -> List[str]:
    resp = await ask_bot("SangMata_beta_bot", username, timeout=12)
    history = []
    if resp:
        lines = resp.split('\n')
        for line in lines:
            if 'было: @' in line or 'Previous username:' in line:
                match = re.search(r'@([a-zA-Z0-9_]+)', line)
                if match:
                    history.append(f"@{match.group(1)}")
            if 'было: ' in line and '@' not in line:
                match = re.search(r'было:\s*(.+)', line)
                if match:
                    history.append(match.group(1).strip())
    return list(set(history))

# ========== 5. ПРЯМОЙ API TELETHON ==========
async def get_direct_info(target: str) -> Dict[str, Any]:
    res = {
        "id": None, "first_name": None, "last_name": None, "username": None,
        "bio": None, "premium": False, "common_chats": [], "reg_date": None, "status_text": None
    }
    try:
        entity = await user_client.get_entity(target)
        full = await user_client(GetFullUserRequest(entity.id))
        user = full.user
        res["id"] = user.id
        res["first_name"] = user.first_name
        res["last_name"] = user.last_name
        res["username"] = f"@{user.username}" if user.username else None
        res["bio"] = full.about
        res["premium"] = getattr(user, 'premium', False)
        res["status_text"] = get_activity_status(user.status)
        if hasattr(user, 'date') and user.date:
            res["reg_date"] = user.date.strftime("%Y-%m-%d")
        common = await user_client.get_common_chats(entity)
        res["common_chats"] = [chat.title for chat in common[:10]]
    except Exception as e:
        logger.error(f"Direct API error: {e}")
    return res

def get_activity_status(status) -> str:
    if hasattr(status, 'expires'):
        return "🟢 сейчас онлайн"
    elif hasattr(status, 'was_online'):
        days_ago = (datetime.now() - status.was_online).days
        if days_ago == 0: return "🟢 был сегодня"
        elif days_ago < 7: return "🟡 был на этой неделе"
        else: return "🔴 более 7 дней назад"
    elif isinstance(status, UserStatusRecently): return "🟡 был недавно (до месяца)"
    elif isinstance(status, UserStatusLastWeek): return "🟠 был на прошлой неделе"
    elif isinstance(status, UserStatusLastMonth): return "🔴 был в прошлом месяце"
    elif isinstance(status, UserStatusEmpty): return "⚪ скрыто / неизвестно"
    return "⚪ неизвестно"

def extract_social_links(text: str) -> List[str]:
    if not text: return []
    links = []
    patterns = [
        (r'github\.com/([a-zA-Z0-9_-]+)', 'GitHub'),
        (r'twitter\.com/([a-zA-Z0-9_-]+)', 'Twitter/X'),
        (r'instagram\.com/([a-zA-Z0-9_.]+)', 'Instagram'),
        (r't\.me/([a-zA-Z0-9_]+)', 'Telegram канал'),
        (r'youtube\.com/@([a-zA-Z0-9_-]+)', 'YouTube'),
        (r'tiktok\.com/@([a-zA-Z0-9_.]+)', 'TikTok'),
        (r'vk\.com/([a-zA-Z0-9_.]+)', 'VK'),
        (r'reddit\.com/user/([a-zA-Z0-9_-]+)', 'Reddit')
    ]
    for pattern, platform in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            links.append(f"{platform}: {m.group(1)}")
    return links

def get_channel_from_bio(bio: str) -> Optional[str]:
    if not bio: return None
    m = re.search(r'(?:t\.me|telegram\.me)/([a-zA-Z0-9_]+)', bio, re.I)
    if m: return f"@{m.group(1)}"
    m = re.search(r'@([a-zA-Z0-9_]{5,})', bio)
    if m and m.group(1) not in ['everyone', 'username', 'telegram']:
        return f"@{m.group(1)}"
    return None

# ========== 6. EMAIL ИЗ ОТВЕТОВ БОТОВ И BIO ==========
async def extract_emails_from_bots(username: str, bio: str) -> List[str]:
    emails = []
    for bot in ["liuofxnhvm3dvqbot", "dateregbot"]:
        resp = await ask_bot(bot, username, timeout=10)
        if resp:
            emails.extend(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', resp))
    if bio:
        emails.extend(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', bio))
    return list(set(emails))

# ========== 7. ПРЯМАЯ ПРОВЕРКА СОЦСЕТЕЙ (HTTP) ==========
async def check_social_networks_direct(username: str) -> Dict[str, str]:
    results = {}
    platforms = {
        "GitHub": f"https://github.com/{username}",
        "Instagram": f"https://instagram.com/{username}",
        "Twitter": f"https://twitter.com/{username}",
        "VK": f"https://vk.com/{username}",
        "Reddit": f"https://reddit.com/user/{username}",
        "YouTube": f"https://youtube.com/@{username}",
    }
    for name, url in platforms.items():
        try:
            async with aiohttp_session.get(url, timeout=5, allow_redirects=True) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    if "not found" not in text.lower() and "doesn't exist" not in text.lower():
                        results[name] = url
        except:
            pass
    return results

# ========== 8. MAIGRET (ПОИСК НА 500+ САЙТАХ) ==========
async def search_maigret(username: str) -> List[str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "maigret", username, "--json", "--max-sites", "300", "-t", "15",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=25)
        data = json.loads(stdout)
        found = []
        for site, info in data.get("sites", {}).items():
            if info.get("status", {}).get("exists"):
                url = info.get("url")
                if url:
                    found.append(f"{site}: {url}")
        return found[:30]
    except FileNotFoundError:
        logger.debug("Maigret не установлен, пропускаем")
        return []
    except Exception as e:
        logger.debug(f"Maigret ошибка: {e}")
        return []

# ========== 9. ОСНОВНАЯ ФУНКЦИЯ СБОРА (ПОСЛЕДОВАТЕЛЬНО) ==========
async def collect_full_info(username: str) -> Dict[str, Any]:
    if username in cache and cache[username]["expires"] > datetime.now():
        return cache[username]["data"]

    result = {
        "username": username,
        "phone": None,
        "vektok": {},
        "direct": {},
        "reg_date_bot": None,
        "name_history": [],
        "emails": [],
        "social_networks": {},
        "maigret_sites": []
    }

    # 1. Номер телефона (один бот)
    phone = await get_phone(username)
    if phone:
        result["phone"] = phone
        result["vektok"] = await get_vektok_info(phone)
    else:
        result["phone"] = None

    # 2. Прямой API Telegram
    result["direct"] = await get_direct_info(username)

    # 3. Дата регистрации от dateregbot
    reg_date = await get_registration_date(username)
    if reg_date:
        result["reg_date_bot"] = reg_date
    await asyncio.sleep(BOT_REQUEST_DELAY)

    # 4. История смены от SangMata
    result["name_history"] = await get_name_history(username)
    await asyncio.sleep(BOT_REQUEST_DELAY)

    # 5. Email
    result["emails"] = await extract_emails_from_bots(username, result["direct"].get("bio"))
    await asyncio.sleep(BOT_REQUEST_DELAY)

    # 6. Прямые соцсети (HTTP)
    result["social_networks"] = await check_social_networks_direct(username)

    # 7. Maigret
    maigret_res = await search_maigret(username)
    if maigret_res:
        result["maigret_sites"] = maigret_res

    cache[username] = {"data": result, "expires": datetime.now() + timedelta(seconds=CACHE_TTL_SECONDS)}
    return result

# ========== 10. ФОРМИРОВАНИЕ ОТЧЁТА ==========
def make_report(info: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"🔍 **OSINT-отчёт** | `{info['username']}`")
    lines.append("═══════════════════════════════════")
    lines.append("")

    if info.get("phone"):
        lines.append(f"📞 **Номер:** `{info['phone']}`")
        v = info["vektok"]
        if v.get("operator"): lines.append(f"📡 **Оператор:** {v['operator']}")
        if v.get("region"): lines.append(f"🌍 **Регион:** {v['region']}")
        if v.get("full_name"): lines.append(f"👤 **ФИО (базы):** {v['full_name']}")
        if v.get("bases_count"): lines.append(f"🗄️ **Базы:** {v['bases_count']} записей")
        if v.get("address"): lines.append(f"🏠 **Адрес:** {v['address']}")
    else:
        lines.append("📞 **Номер:** не найден")
    lines.append("")

    d = info["direct"]
    if d.get("id"): lines.append(f"🆔 **ID:** `{d['id']}`")
    if d.get("first_name"): lines.append(f"👤 **Имя:** {d['first_name']} {d['last_name'] or ''}")
    if d.get("username"): lines.append(f"📛 **Юзернейм:** {d['username']}")
    if d.get("premium"): lines.append("⭐ **Telegram Premium**")
    if d.get("reg_date"): lines.append(f"📅 **Регистрация (API):** {d['reg_date']}")
    if info["reg_date_bot"]: lines.append(f"📅 **Регистрация (бот):** {info['reg_date_bot']}")
    if d.get("status_text"): lines.append(f"🕒 **Активность:** {d['status_text']}")

    if d.get("bio"):
        bio = d['bio'][:200] + "..." if len(d['bio']) > 200 else d['bio']
        lines.append(f"📝 **Bio:** {bio}")
        social_links = extract_social_links(d['bio'])
        if social_links:
            lines.append("🌐 **Ссылки в bio:**")
            for link in social_links[:5]:
                lines.append(f"   • {link}")
        channel = get_channel_from_bio(d['bio'])
        if channel:
            lines.append(f"📢 **Канал в bio:** {channel}")

    if d.get("common_chats"):
        lines.append(f"👥 **Общие группы:** {len(d['common_chats'])}")
        for chat in d['common_chats'][:3]:
            lines.append(f"   • {chat}")
        if len(d['common_chats']) > 3:
            lines.append(f"   • и ещё {len(d['common_chats'])-3}...")

    if info["name_history"]:
        lines.append(f"🔄 **История смены (SangMata):**")
        for item in info["name_history"][:5]:
            lines.append(f"   • {item}")

    if info["emails"]:
        lines.append("📧 **Найденные email:**")
        for email in info['emails'][:3]:
            lines.append(f"   • `{email}`")

    if info["social_networks"]:
        lines.append("🌎 **Найден в соцсетях (HTTP):**")
        for platform, url in info["social_networks"].items():
            lines.append(f"   • {platform}: {url}")

    if info["maigret_sites"]:
        lines.append("🌍 **Другие платформы (Maigret):**")
        for site in info["maigret_sites"][:10]:
            lines.append(f"   • {site}")

    if not info["phone"] and not d.get("id") and not info["name_history"]:
        lines.append("\n❌ Информация не найдена.")
    return "\n".join(lines)

# ========== 11. ОБРАБОТЧИКИ БОТА (aiogram 3.x) ==========
@dp.message(Command('start'))
async def cmd_start(message: types.Message):
    await message.reply(
        "🔮 *OSINT бот (без req_id, без глобального хендлера)*\n\n"
        "Отправь юзернейм (можно с @ или без – я отправлю как есть).\n"
        "Собираю: номер телефона, оператор, регион, ФИО, адрес,\n"
        "ID, имя, активность, общие группы, дату регистрации,\n"
        "историю смены имени/юзернейма, email, соцсети.\n"
        "**Maigret** ищет профили на 500+ сайтах (если установлен).\n"
        "⏳ Ожидание до 90 секунд.\n"
        "⚠️ Не более 1 запроса в 15 секунд.",
        parse_mode="Markdown"
    )

@dp.message()
async def handle_username(message: types.Message):
    user_id = message.from_user.id
    now = datetime.now()
    if user_id in user_last_request and (now - user_last_request[user_id]).seconds < USER_RATE_LIMIT_SECONDS:
        await message.reply("⏳ Слишком много запросов. Подождите 15 секунд.")
        return
    user_last_request[user_id] = now

    target = message.text.strip()   # НЕ удаляем @, оставляем как есть
    if not target:
        await message.reply("❌ Введите юзернейм.")
        return

    lock = user_locks.setdefault(target, asyncio.Lock())
    async with lock:
        status_msg = await message.reply("🔎 Сбор данных... до 90 секунд")
        async with global_semaphore:
            info = await collect_full_info(target)
        report = make_report(info)
        kb = None
        if info["direct"].get("username"):
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Открыть профиль", url=f"https://t.me/{info['direct']['username'].lstrip('@')}")]
            ])
        await status_msg.edit_text(report, parse_mode="Markdown", reply_markup=kb, disable_web_page_preview=True)

# ========== 12. ЗАПУСК WEBHOOK И TELETHON ==========
async def health(request):
    return web.Response(text="OK")

async def webhook(request):
    data = await request.json()
    update = types.Update(**data)
    await dp.feed_update(bot, update)
    return web.Response(text="OK")

async def on_startup():
    await bot.set_webhook(f"{BASE_URL}{WEBHOOK_PATH}")
    logger.info(f"Webhook установлен: {BASE_URL}{WEBHOOK_PATH}")

async def run_telethon():
    while True:
        try:
            await user_client.start()
            logger.info("Telethon подключён")
            await user_client.run_until_disconnected()
        except Exception as e:
            logger.error(f"Telethon упал: {e}")
            await asyncio.sleep(10)

async def main():
    global aiohttp_session
    aiohttp_session = aiohttp.ClientSession()
    asyncio.create_task(run_telethon())
    await asyncio.sleep(2)
    await user_client.start()
    me = await user_client.get_me()
    logger.info(f"✅ Telethon активен от @{me.username}")

    # Предзагрузка ID всех используемых ботов
    for bot_name in ["liuofxnhvm3dvqbot", "vektokOsint_bot", "dateregbot", "SangMata_beta_bot"]:
        await get_bot_id(bot_name)

    await on_startup()
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post(WEBHOOK_PATH, webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Aiohttp сервер запущен на порту {PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
