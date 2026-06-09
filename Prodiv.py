#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import re
import logging
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
BOT_REQUEST_DELAY = 1.5

if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
    raise Exception("❌ Задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== ОГРАНИЧИТЕЛИ ==========
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_USERS)
user_locks: Dict[str, asyncio.Lock] = {}
user_last_request: Dict[int, datetime] = {}
cache: Dict[str, dict] = {}
aiohttp_session: Optional[aiohttp.ClientSession] = None

# ========== КЭШ ID БОТОВ ==========
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

# ========== УНИВЕРСАЛЬНЫЙ ask_bot (синхронный handler) ==========
async def ask_bot(bot_username: str, query: str, timeout: int = 15, collect_messages: int = 1) -> Optional[str]:
    bot_id = await get_bot_id(bot_username)
    if not bot_id:
        return None

    loop = asyncio.get_event_loop()
    future = loop.create_future()
    messages = []

    def handler(event):
        text = ""
        if hasattr(event.message, 'message') and event.message.message:
            text = event.message.message
        elif event.raw_text:
            text = event.raw_text
        else:
            return
        
        if text:
            logger.debug(f"[{bot_username}] Получено сообщение: {text[:100]}")
            messages.append(text)
            if len(messages) >= collect_messages:
                if not future.done():
                    loop.call_soon_threadsafe(future.set_result, "\n".join(messages))
                user_client.remove_event_handler(handler)

    user_client.add_event_handler(handler, events.NewMessage(from_users=bot_id))

    try:
        entity = await user_client.get_entity(bot_username)
        await user_client.send_message(entity, query)
        return await asyncio.wait_for(future, timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут {bot_username}")
        return None
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds} сек для {bot_username}")
        await asyncio.sleep(e.seconds + 0.5)
        return await ask_bot(bot_username, query, timeout, collect_messages)
    except Exception as e:
        logger.error(f"Ошибка {bot_username}: {e}")
        return None
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass

# ========== ФИЛЬТРАЦИЯ МУСОРНЫХ СООБЩЕНИЙ ==========
def clean_bot_text(text: str) -> str:
    if not text:
        return ""
    lines = text.split("\n")
    bad_words = ["search", "wait", "loading", "please", "checking", "ищем", "подождите", "загрузка"]
    clean_lines = []
    for line in lines:
        if not any(bad in line.lower() for bad in bad_words):
            clean_lines.append(line)
    return "\n".join(clean_lines)

# ========== НОРМАЛИЗАЦИЯ ID ==========
def normalize_user_id(resp: str) -> Optional[int]:
    if not resp:
        return None
    patterns = [
        r'ID:\s*(\d+)',
        r'User ID:\s*(\d+)',
        r'User id:\s*(\d+)',
        r'\b(\d{6,15})\b'
    ]
    for p in patterns:
        m = re.search(p, resp, re.I)
        if m:
            return int(m.group(1))
    return None

# ========== 1. НОМЕР ТЕЛЕФОНА ==========
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

# ========== 2. ДАННЫЕ ОТ @vektokOsint_bot (СТАБИЛЬНАЯ ВЕРСИЯ) ==========
async def get_vektok_info(phone: str) -> Dict[str, Any]:
    if not phone or not re.search(r'\d{10,}', phone):
        return {}

    result = {
        "operator": "", "region": "", "full_name": "",
        "bases_count": "", "records_count": "", "address": ""
    }
    bot_id = await get_bot_id("vektokOsint_bot")
    if not bot_id:
        return result

    loop = asyncio.get_event_loop()
    future = loop.create_future()

    def handler(event):
        if future.done():
            return

        msg = event.message

        # 📄 Если пришёл документ (файл)
        if msg.document:
            asyncio.create_task(process_file(event))
            return

        # 💬 Если пришёл текст (HTML прямо в сообщении)
        text = msg.message or msg.raw_text or ""
        if text:
            logger.debug(f"[vektok] Получен текст: {text[:200]}")
            parsed = parse_vektok_html(text)
            if not future.done():
                loop.call_soon_threadsafe(future.set_result, parsed)

    async def process_file(event):
        file_path = None
        try:
            file_path = await event.message.download_media()
            with open(file_path, 'rb') as f:
                data = f.read()
            
            html = data.decode('utf-8', errors='ignore')
            parsed = parse_vektok_html(html)
            if not future.done():
                future.set_result(parsed)
        except Exception as e:
            logger.error(f"Vektok file error: {e}")
        finally:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass

    user_client.add_event_handler(handler, events.NewMessage(from_users=bot_id))

    try:
        entity = await user_client.get_entity("vektokOsint_bot")
        await user_client.send_message(entity, phone)
        parsed = await asyncio.wait_for(future, timeout=30)
        if parsed:
            result.update(parsed)
    except asyncio.TimeoutError:
        logger.warning("Таймаут vektokOsint_bot")
    except Exception as e:
        logger.error(f"Ошибка vektok: {e}")
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass
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

# ========== 3. ПОЛУЧЕНИЕ ID ==========
async def get_user_id(username: str) -> Optional[int]:
    resp = await ask_bot("username_to_id_bot", username, timeout=15)
    logger.info(f"[DEBUG] Ответ username_to_id_bot: {resp}")
    return normalize_user_id(resp)

# ========== 4. ИСТОРИЯ СМЕНЫ (SangMata) ==========
def clean_sangmata_text(text: str) -> str:
    if not text:
        return ""
    lines = text.split("\n")
    bad_words = ["search", "wait", "loading", "please", "checking", "ищем", "подождите", "загрузка", "загружаю"]
    clean_lines = []
    for line in lines:
        if not any(bad in line.lower() for bad in bad_words):
            clean_lines.append(line)
    return "\n".join(clean_lines)

async def get_name_history_by_id(user_id: int) -> List[str]:
    await asyncio.sleep(2)
    logger.info(f"[DEBUG] SangMata запрос ID: {user_id}")
    
    resp = await ask_bot("SangMata_beta_bot", str(user_id), timeout=25, collect_messages=3)
    logger.info(f"[DEBUG] SangMata RAW ответ: {resp}")
    
    if not resp:
        return []
    
    resp = clean_sangmata_text(resp)
    logger.info(f"[DEBUG] SangMata после фильтрации: {resp}")
    
    lines = resp.split('\n')
    history = []
    
    for line in lines:
        line = line.strip()
        
        if "было:" in line.lower() or "previous" in line.lower():
            match = re.search(r'@([a-zA-Z0-9_]+)', line)
            if match:
                history.append(f"@{match.group(1)}")
        
        if "было:" in line.lower() and "@" not in line:
            m = re.search(r'было:\s*(.+)', line, re.I)
            if m:
                history.append(m.group(1).strip())
    
    return list(dict.fromkeys(history))

# ========== 5. ДАННЫЕ ОТ @dateregbot ==========
def parse_dates(text: str) -> Dict[str, Optional[str]]:
    result = {
        "user_id": None,
        "reg": None,
        "seen": None,
        "accuracy": None
    }
    if not text:
        return result

    m = re.search(r'ID:\s*(\d+)', text)
    if m:
        result["user_id"] = m.group(1)

    m = re.search(r'Регистрация:\s*([^\n]+)', text)
    if m:
        result["reg"] = m.group(1).strip()

    m = re.search(r'Появление:\s*([^\n]+)', text)
    if m:
        result["seen"] = m.group(1).strip()

    m = re.search(r'Точность:\s*([^\n]+)', text)
    if m:
        result["accuracy"] = m.group(1).strip()

    return result

async def get_registration_date(username: str) -> Dict[str, Optional[str]]:
    resp = await ask_bot("dateregbot", username, timeout=30, collect_messages=4)
    logger.info(f"[DEBUG] dateregbot RAW ответ: {resp}")
    if resp:
        resp = clean_bot_text(resp)
        logger.info(f"[DEBUG] dateregbot после фильтрации: {resp}")
    return parse_dates(resp)

# ========== 6. ПРЯМОЙ API ==========
async def get_direct_info(target: str) -> Dict[str, Any]:
    res = {
        "id": None, "first_name": None, "last_name": None, "username": None,
        "bio": None, "premium": False, "common_chats": [], "reg_date": None, "status_text": None,
        "verified": False, "scam": False, "fake": False, "bot": False,
        "dc_id": None, "has_avatar": False, "usernames": []
    }
    try:
        entity = await user_client.get_entity(target)
        full = await user_client(GetFullUserRequest(entity.id))
        
        if hasattr(full, 'users') and full.users:
            user = full.users[0]
        elif hasattr(full, 'full_user'):
            user = full.full_user
        else:
            logger.error("Не удалось получить объект user")
            return res
        
        res["id"] = user.id
        res["first_name"] = user.first_name
        res["last_name"] = user.last_name
        res["username"] = f"@{user.username}" if user.username else None
        
        if hasattr(user, 'usernames') and user.usernames:
            res["usernames"] = [f"@{u.username}" for u in user.usernames]
        
        res["bio"] = full.about
        res["premium"] = getattr(user, 'premium', False)
        res["verified"] = getattr(user, 'verified', False)
        res["scam"] = getattr(user, 'scam', False)
        res["fake"] = getattr(user, 'fake', False)
        res["bot"] = getattr(user, 'bot', False)
        res["status_text"] = get_activity_status(user.status)
        
        if hasattr(user, 'photo') and user.photo:
            res["has_avatar"] = True
            if hasattr(user.photo, 'dc_id'):
                res["dc_id"] = user.photo.dc_id
        
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
        if days_ago == 0:
            return "🟢 был сегодня"
        elif days_ago < 7:
            return "🟡 был на этой неделе"
        else:
            return "🔴 более 7 дней назад"
    elif isinstance(status, UserStatusRecently):
        return "🟡 был недавно (до месяца)"
    elif isinstance(status, UserStatusLastWeek):
        return "🟠 был на прошлой неделе"
    elif isinstance(status, UserStatusLastMonth):
        return "🔴 был в прошлом месяце"
    elif isinstance(status, UserStatusEmpty):
        return "⚪ скрыто / неизвестно"
    return "⚪ неизвестно"

def extract_social_links(text: str) -> List[str]:
    if not text:
        return []
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
    if not bio:
        return None
    m = re.search(r'(?:t\.me|telegram\.me)/([a-zA-Z0-9_]+)', bio, re.I)
    if m:
        return f"@{m.group(1)}"
    m = re.search(r'@([a-zA-Z0-9_]{5,})', bio)
    if m and m.group(1) not in ['everyone', 'username', 'telegram']:
        return f"@{m.group(1)}"
    return None

# ========== 7. EMAIL ==========
async def extract_emails_from_bots(username: str, bio: str) -> List[str]:
    emails = []
    for bot in ["liuofxnhvm3dvqbot", "username_to_id_bot"]:
        resp = await ask_bot(bot, username, timeout=10)
        if resp:
            emails.extend(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', resp))
    if bio:
        emails.extend(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', bio))
    return list(set(emails))

# ========== 8. СОЦСЕТИ ==========
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

# ========== 9. MAIGRET ==========
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

# ========== 10. ОСНОВНАЯ ФУНКЦИЯ СБОРА ==========
async def collect_full_info(username: str) -> Dict[str, Any]:
    if username in cache and cache[username]["expires"] > datetime.now():
        return cache[username]["data"]

    result = {
        "username": username,
        "phone": None,
        "vektok": {},
        "user_id": None,
        "name_history": [],
        "dateregbot": {},
        "direct": {},
        "emails": [],
        "social_networks": {},
        "maigret_sites": []
    }

    # ШАГ 1: номер телефона
    phone = await get_phone(username)
    if phone and re.search(r'\d{10,}', phone):
        result["phone"] = phone
        result["vektok"] = await get_vektok_info(phone)
        await asyncio.sleep(BOT_REQUEST_DELAY)

    # ШАГ 2: получаем ID
    user_id = await get_user_id(username)
    if user_id:
        result["user_id"] = user_id
        await asyncio.sleep(2)
        result["name_history"] = await get_name_history_by_id(user_id)
        await asyncio.sleep(BOT_REQUEST_DELAY)

    # ШАГ 3: dateregbot
    result["dateregbot"] = await get_registration_date(username)
    await asyncio.sleep(BOT_REQUEST_DELAY)

    # ШАГ 4: прямой API
    result["direct"] = await get_direct_info(username)

    # ШАГ 5: email
    result["emails"] = await extract_emails_from_bots(username, result["direct"].get("bio"))
    await asyncio.sleep(BOT_REQUEST_DELAY)

    # ШАГ 6: соцсети
    result["social_networks"] = await check_social_networks_direct(username)

    # ШАГ 7: Maigret
    maigret_res = await search_maigret(username)
    if maigret_res:
        result["maigret_sites"] = maigret_res

    cache[username] = {"data": result, "expires": datetime.now() + timedelta(seconds=CACHE_TTL_SECONDS)}
    return result

# ========== 11. ОТЧЁТ ==========
def make_report(info: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"🔍 <b>OSINT-отчёт</b> | <code>{info['username']}</code>")
    lines.append("═══════════════════════════════════")
    lines.append("")

    # Блок номера и vektok
    if info.get("phone"):
        lines.append(f"📞 <b>Номер:</b> <code>{info['phone']}</code>")
        v = info["vektok"]
        if v.get("operator"):
            lines.append(f"📡 <b>Оператор:</b> {v['operator']}")
        if v.get("region"):
            lines.append(f"🌍 <b>Регион:</b> {v['region']}")
        if v.get("full_name"):
            lines.append(f"👤 <b>ФИО (базы):</b> {v['full_name']}")
        if v.get("bases_count"):
            lines.append(f"🗄️ <b>Базы:</b> {v['bases_count']} записей")
        if v.get("address"):
            lines.append(f"🏠 <b>Адрес:</b> {v['address']}")
    else:
        lines.append("📞 <b>Номер:</b> не найден")
    lines.append("")

    # Блок dateregbot
    dater = info.get("dateregbot", {})
    if dater.get("user_id") or dater.get("reg") or dater.get("seen"):
        lines.append("⭐️ <b>Информация от dateregbot</b>")
        if dater.get("user_id"):
            lines.append(f"🆔 ID: <code>{dater['user_id']}</code>")
        if dater.get("reg"):
            lines.append(f"📅 Регистрация: {dater['reg']}")
        if dater.get("seen"):
            lines.append(f"👀 Появление: {dater['seen']}")
        if dater.get("accuracy"):
            lines.append(f"🎯 Точность: {dater['accuracy']}")
        lines.append("")

    # Блок из прямого API
    d = info["direct"]
    if d.get("usernames") and len(d["usernames"]) > 1:
        lines.append(f"📛 <b>Активные username:</b> {', '.join(d['usernames'])}")
    elif d.get("username"):
        lines.append(f"📛 <b>Юзернейм:</b> {d['username']}")
    
    if d.get("id"):
        lines.append(f"🆔 <b>ID:</b> <code>{d['id']}</code>")
    if d.get("first_name"):
        lines.append(f"👤 <b>Имя:</b> {d['first_name']} {d['last_name'] or ''}")
    if d.get("verified"):
        lines.append("✅ <b>Верифицирован:</b> ДА")
    if d.get("scam"):
        lines.append("⚠️ <b>Scam:</b> ДА")
    if d.get("fake"):
        lines.append("⚠️ <b>Fake:</b> ДА")
    if d.get("bot"):
        lines.append("🤖 <b>Бот:</b> ДА")
    else:
        lines.append("🤖 <b>Бот:</b> НЕТ")
    if d.get("premium"):
        lines.append("⭐ <b>Telegram Premium</b>")
    if d.get("dc_id"):
        lines.append(f"🌐 <b>Дата-центр (DC):</b> {d['dc_id']}")
    if d.get("has_avatar"):
        lines.append("🖼 <b>Аватар:</b> есть")
    else:
        lines.append("🖼 <b>Аватар:</b> нет")
    if d.get("reg_date"):
        lines.append(f"📅 <b>Регистрация (API):</b> {d['reg_date']}")
    if d.get("status_text"):
        lines.append(f"🕒 <b>Активность:</b> {d['status_text']}")

    if d.get("bio"):
        bio = d['bio'][:200] + "..." if len(d['bio']) > 200 else d['bio']
        lines.append(f"📝 <b>Bio:</b> {bio}")
        social_links = extract_social_links(d['bio'])
        if social_links:
            lines.append("🌐 <b>Ссылки в bio:</b>")
            for link in social_links[:5]:
                lines.append(f"   • {link}")
        channel = get_channel_from_bio(d['bio'])
        if channel:
            lines.append(f"📢 <b>Канал в bio:</b> {channel}")

    if d.get("common_chats"):
        lines.append(f"👥 <b>Общие группы:</b> {len(d['common_chats'])}")
        for chat in d['common_chats'][:3]:
            lines.append(f"   • {chat}")
        if len(d['common_chats']) > 3:
            lines.append(f"   • и ещё {len(d['common_chats'])-3}...")

    # История смены от SangMata
    if info["name_history"]:
        lines.append(f"🔄 <b>История смены (SangMata):</b>")
        for item in info["name_history"][:10]:
            lines.append(f"   • {item}")

    # Email
    if info["emails"]:
        lines.append("📧 <b>Найденные email:</b>")
        for email in info['emails'][:3]:
            lines.append(f"   • <code>{email}</code>")

    # Соцсети
    if info["social_networks"]:
        lines.append("🌎 <b>Найден в соцсетях (HTTP):</b>")
        for platform, url in info["social_networks"].items():
            lines.append(f"   • {platform}: {url}")

    # Maigret
    if info["maigret_sites"]:
        lines.append("🌍 <b>Другие платформы (Maigret):</b>")
        for site in info["maigret_sites"][:10]:
            lines.append(f"   • {site}")

    if not info["phone"] and not d.get("id") and not dater.get("user_id"):
        lines.append("\n❌ Информация не найдена.")
    return "\n".join(lines)

# ========== 12. ОБРАБОТЧИКИ ==========
@dp.message(Command('start'))
async def cmd_start(message: types.Message):
    await message.reply(
        "🔮 <b>OSINT бот (PRO версия, полный отчёт)</b>\n\n"
        "Отправь юзернейм.\n\n"
        "<b>Цепочка:</b>\n"
        "1️⃣ @liuofxnhvm3dvqbot → номер телефона\n"
        "2️⃣ @vektokOsint_bot → оператор, регион, ФИО, адрес\n"
        "3️⃣ @username_to_id_bot → числовой ID\n"
        "4️⃣ @SangMata_beta_bot → история смены (по ID)\n"
        "5️⃣ @dateregbot → дата регистрации и первое появление\n"
        "6️⃣ Прямой API Telegram → ID, имя, верификация, Premium, DC, аватар\n"
        "7️⃣ Поиск email и соцсетей\n"
        "8️⃣ Maigret → профили на 500+ сайтах\n\n"
        "⏳ Ожидание до 90 секунд.\n"
        "⚠️ Не более 1 запроса в 15 секунд.",
        parse_mode="HTML"
    )

@dp.message()
async def handle_username(message: types.Message):
    user_id = message.from_user.id
    now = datetime.now()
    if user_id in user_last_request and (now - user_last_request[user_id]).seconds < USER_RATE_LIMIT_SECONDS:
        await message.reply("⏳ Слишком много запросов. Подождите 15 секунд.")
        return
    user_last_request[user_id] = now

    target = message.text.strip()
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
        await status_msg.edit_text(report, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)

# ========== 13. ЗАПУСК ==========
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

    for bot_name in ["liuofxnhvm3dvqbot", "vektokOsint_bot", "username_to_id_bot", "SangMata_beta_bot", "dateregbot"]:
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
