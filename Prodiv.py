#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sys
import re
import sqlite3
import logging
import json
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from threading import Thread
from typing import Optional, List, Dict, Any
from ipaddress import ip_address

import requests
from flask import Flask
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetCommonChatsRequest, GetStickerSetsRequest, GetHistoryRequest
from telethon.tl.functions.account import GetAuthorizationsRequest, GetNotifySettingsRequest
from telethon.tl.functions.contacts import GetBlockedRequest, GetContactsRequest
from telethon.tl.types import (
    InputUserSelf, InputPeerUser, UserStatusOnline, UserStatusOffline,
    MessageActionGiftCode, MessageActionStarGift, MessageActionPaymentSent,
    MessageActionPinMessage, ChannelParticipant, ChannelParticipantBanned,
    StickerSet, StickerSetCovered, UserFull, Contact
)

# Опциональные библиотеки
try:
    from telegram_gift_fetcher import get_user_gifts
    HAS_GIFTS = True
except ImportError:
    HAS_GIFTS = False

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# ========== ВЕБ-СЕРВЕР ДЛЯ UPTIMEROBOT ==========
flask_app = Flask('')

@flask_app.route('/')
def health():
    return "OK"

def run_flask():
    flask_app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))

# ========== ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ==========
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

if not BOT_TOKEN or not API_ID or not API_HASH or not SESSION_STRING:
    print("Ошибка: не заданы BOT_TOKEN, API_ID, API_HASH или SESSION_STRING")
    sys.exit(1)

# ========== НАСТРОЙКИ ==========
DAYS_BACK = 30
MAX_MESSAGES_PER_CHAT = 2000
MAX_CHATS = 20

WEIGHTS = {
    'message_count': 0.2,
    'reaction_count': 0.5,
    'avg_msg_length': 0.2,
    'media_ratio': 0.05,
    'link_ratio': 0.05
}
MAX_EXPECTED = {
    'message_count': 1000,
    'reaction_count': 500,
    'avg_msg_length': 100,
    'media_ratio': 0.5,
    'link_ratio': 0.2
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== БАЗА ДАННЫХ ==========
DB_PATH = "user_data.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS username_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        old_username TEXT,
        new_username TEXT,
        change_date TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS name_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        old_name TEXT,
        new_name TEXT,
        change_date TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS avatar_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        avatar_hash TEXT,
        change_date TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS sessions (
        user_id INTEGER,
        device TEXT,
        ip TEXT,
        last_active TEXT,
        created_at TEXT
    )''')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

init_db()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def extract_links(text: str) -> int:
    return len(re.findall(r'https?://[^\s]+', text)) if text else 0

def dc_from_avatar(avatar_url: str) -> int:
    if not avatar_url:
        return None
    match = re.search(r'cdn(\d)\.', avatar_url)
    if match:
        mapping = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}
        return mapping.get(int(match.group(1)))
    return None

def parse_status(status) -> str:
    if status is None:
        return "скрыт"
    if isinstance(status, UserStatusOnline):
        return "онлайн"
    if isinstance(status, UserStatusOffline):
        if status.was_online:
            return f"был в сети {status.was_online.strftime('%Y-%m-%d %H:%M:%S')}"
        return "оффлайн"
    return str(status)

def estimate_age_by_id(user_id: int) -> str:
    if user_id < 10000000:
        return "2013-2014"
    elif user_id < 100000000:
        return "2014-2016"
    elif user_id < 500000000:
        return "2016-2018"
    elif user_id < 2000000000:
        return "2018-2021"
    elif user_id < 5000000000:
        return "2021-2024"
    elif user_id < 7000000000:
        return "2024-2025"
    else:
        return "2025-2026"

def estimate_gender_by_name(first_name: str) -> str:
    if not first_name:
        return "не определён"
    name_lower = first_name.lower()
    male_ends = ('й', 'н', 'л', 'р', 'в', 'к', 'м', 'п', 'т', 'ч')
    female_ends = ('а', 'я', 'ия', 'ь')
    if name_lower.endswith(male_ends) and not name_lower.endswith(female_ends):
        return "Мужской"
    elif name_lower.endswith(female_ends):
        return "Женский"
    else:
        return "не определён"

def get_geo_by_ip(ip: str) -> str:
    """Простая геолокация по IP (без API ключа)"""
    if not ip or ip.startswith('127.') or ip.startswith('192.168.'):
        return "локальный IP"
    try:
        # Определяем страну по диапазонам (упрощённо)
        first_octet = int(ip.split('.')[0])
        if first_octet == 185 or first_octet == 213:
            return "Казахстан, Алматы (предположительно)"
        elif first_octet == 149 and ip.startswith('149.154'):
            return "Нидерланды, Амстердам (дата-центр Telegram)"
        elif first_octet == 91 or first_octet == 95:
            return "Россия"
        elif first_octet == 46 or first_octet == 78:
            return "Германия"
        else:
            return "неизвестно"
    except:
        return "неизвестно"

# ========== КЛИЕНТ TELETHON ==========
async def create_client():
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    logger.info("Клиент авторизован")
    return client

async def keep_alive(client):
    while True:
        await asyncio.sleep(300)
        try:
            await client.get_me()
        except Exception as e:
            logger.error(f"Keep-alive ошибка: {e}")

# ========== РАСШИРЕННЫЙ СБОР ДАННЫХ ==========
async def get_user_sessions(client, user_id) -> List[Dict]:
    try:
        result = await client(GetAuthorizationsRequest())
        sessions = []
        for auth in result.authorizations:
            sessions.append({
                'device': f"{auth.device_model} ({auth.app_name})",
                'ip': auth.ip,
                'last_active': auth.date_active.isoformat(),
                'created_at': auth.date_created.isoformat(),
                'is_current': auth.current
            })
        return sessions
    except Exception as e:
        logger.error(f"Ошибка получения сессий: {e}")
        return []

async def get_blocked_users(client) -> List[int]:
    try:
        result = await client(GetBlockedRequest(offset=0, limit=100))
        return [b.peer_id.user_id for b in result.blocked] if result.blocked else []
    except Exception as e:
        logger.error(f"Ошибка получения заблокированных: {e}")
        return []

async def get_contacts_count(client) -> int:
    try:
        result = await client(GetContactsRequest(hash=0))
        return len(result.users) if result.users else 0
    except Exception as e:
        logger.error(f"Ошибка получения контактов: {e}")
        return 0

async def get_sticker_sets(client) -> List[Dict]:
    try:
        result = await client(GetStickerSetsRequest(featured=False, hash=0))
        stickers = []
        for s in result.sets:
            stickers.append({
                'title': s.title,
                'short_name': s.short_name,
                'count': s.count
            })
        return stickers
    except Exception as e:
        logger.error(f"Ошибка получения стикеров: {e}")
        return []

async def get_notify_settings(client) -> Dict:
    try:
        result = await client(GetNotifySettingsRequest(peer=InputPeerUser(0, 0)))
        return {
            'show_previews': result.show_previews,
            'silent': result.silent,
            'mute_until': result.mute_until.isoformat() if result.mute_until else None,
            'sound': result.sound
        }
    except Exception as e:
        logger.error(f"Ошибка получения настроек: {e}")
        return {}

async def get_pinned_messages(client, chat_id, limit=5) -> List[Dict]:
    try:
        messages = []
        async for msg in client.iter_messages(chat_id, pinned=True, limit=limit):
            messages.append({
                'text': msg.text[:200] if msg.text else '[медиа]',
                'date': msg.date.isoformat(),
                'id': msg.id
            })
        return messages
    except Exception as e:
        logger.error(f"Ошибка получения закреплённых сообщений: {e}")
        return []

async def get_folders_count(client) -> int:
    try:
        # Подсчёт папок через диалоги (приблизительный)
        dialogs = await client.get_dialogs()
        folders = set()
        for d in dialogs:
            if d.folder_id is not None:
                folders.add(d.folder_id)
        return len(folders)
    except:
        return 0

async def get_2fa_info(client) -> Dict:
    try:
        # Проверка наличия 2FA через попытку
        me = await client.get_me()
        return {'enabled': None, 'message': 'невозможно определить через API'}
    except:
        return {'enabled': None, 'message': 'ошибка проверки'}

# ========== СБОР АКТИВНОСТИ ==========
async def collect_activity(client, target_user, common_chats, days_back=30):
    metrics = defaultdict(int)
    word_counter = Counter()
    reaction_details = Counter()
    hourly_activity = Counter()
    daily_activity = Counter()
    cutoff_date = datetime.now() - timedelta(days=days_back)
    processed_chats = 0

    for chat in common_chats[:MAX_CHATS]:
        chat_id = chat['id']
        try:
            async for msg in client.iter_messages(chat_id, from_user=target_user.id, offset_date=cutoff_date, limit=MAX_MESSAGES_PER_CHAT):
                metrics['total_messages'] += 1
                if msg.text:
                    metrics['total_chars'] += len(msg.text)
                    metrics['links'] += extract_links(msg.text)
                    words = re.findall(r'\b[а-яА-Яa-zA-Z]{3,}\b', msg.text.lower())
                    word_counter.update(words)
                if msg.media:
                    metrics['media'] += 1
                    if hasattr(msg.media, 'photo'):
                        metrics['photos'] += 1
                    elif hasattr(msg.media, 'document'):
                        if hasattr(msg.media.document, 'mime_type'):
                            if 'video' in msg.media.document.mime_type:
                                metrics['videos'] += 1
                            elif 'gif' in msg.media.document.mime_type:
                                metrics['gifs'] += 1
                if msg.reactions:
                    for r in msg.reactions.results:
                        metrics['reactions'] += r.count
                        if hasattr(r, 'reaction') and hasattr(r.reaction, 'emoticon'):
                            reaction_details[r.reaction.emoticon] += r.count
                # Временная статистика
                if msg.date:
                    hour = msg.date.hour
                    weekday = msg.date.weekday()
                    hourly_activity[hour] += 1
                    daily_activity[weekday] += 1
            processed_chats += 1
            await asyncio.sleep(0.5)
        except errors.FloodWaitError as e:
            logger.warning(f"FloodWait: ждём {e.seconds} сек")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Ошибка в чате {chat_id}: {e}")

    if metrics['total_messages'] > 0:
        metrics['avg_msg_length'] = metrics['total_chars'] / metrics['total_messages']
        metrics['media_ratio'] = metrics['media'] / metrics['total_messages']
        metrics['link_ratio'] = metrics['links'] / metrics['total_messages']
    else:
        metrics['avg_msg_length'] = 0
        metrics['media_ratio'] = 0
        metrics['link_ratio'] = 0

    return metrics, processed_chats, word_counter, reaction_details, hourly_activity, daily_activity

def calculate_custom_rating(metrics):
    raw = 0
    for metric, weight in WEIGHTS.items():
        value = metrics.get(metric, 0)
        max_val = MAX_EXPECTED.get(metric, 1)
        norm = min(value / max_val, 1.0) if max_val > 0 else 0
        raw += norm * weight
    return round(raw * 100, 2)

# ========== ПОЛУЧЕНИЕ ДАННЫХ ПОЛЬЗОВАТЕЛЯ ==========
async def get_full_user_info(client, target):
    try:
        full = await client(GetFullUserRequest(target))
        user = full.users[0]
        full_user = full.full_user
        bio = getattr(full_user, 'about', None)
        avatar = None
        if full_user.photo and hasattr(full_user.photo, 'sizes') and full_user.photo.sizes:
            avatar = str(full_user.photo.sizes[-1].location)
        dc = None
        if hasattr(user, 'photo') and user.photo and hasattr(user.photo, 'dc_id'):
            dc = user.photo.dc_id
        if not dc:
            dc = dc_from_avatar(avatar) if avatar else None
        official_rating = None
        if hasattr(full_user, 'stars_balance'):
            official_rating = full_user.stars_balance
        return user, bio, avatar, dc, full_user, official_rating
    except Exception as e:
        logger.error(f"Ошибка full info: {e}")
        return target, None, None, None, None, None

async def get_common_chats(client, target_user):
    try:
        common = await client(GetCommonChatsRequest(
            user_id=InputUserSelf(),
            max_id=0,
            limit=100
        ))
        chats = []
        for c in common.chats:
            chats.append({
                'id': c.id,
                'title': getattr(c, 'title', str(c.id)),
                'username': getattr(c, 'username', None),
                'participants_count': getattr(c, 'participants_count', 0)
            })
        return chats
    except Exception as e:
        logger.error(f"Ошибка общих групп: {e}")
        return []

async def get_all_usernames(client, target_user):
    usernames = [target_user.username] if target_user.username else []
    if hasattr(target_user, 'usernames') and target_user.usernames:
        for un in target_user.usernames:
            if un.username and un.username != target_user.username:
                usernames.append(un.username)
    return usernames

async def get_emoji_status(client, target_user):
    if hasattr(target_user, 'emoji_status') and target_user.emoji_status:
        return target_user.emoji_status
    return None

# ========== ПОДАРКИ ==========
async def get_gifts_info(client, user_id):
    if not HAS_GIFTS:
        return None, None
    try:
        gifts = await get_user_gifts(client, user_id)
        sent = []
        received = []
        if gifts:
            if 'sent_gifts' in gifts:
                for g in gifts['sent_gifts']:
                    sent.append({'to_id': g.get('to_id'), 'type': g.get('type'), 'date': g.get('date')})
            if 'received_gifts' in gifts:
                for g in gifts['received_gifts']:
                    received.append({'from_id': g.get('from_id'), 'type': g.get('type'), 'date': g.get('date')})
        return sent, received
    except Exception as e:
        logger.error(f"Ошибка получения подарков: {e}")
        return None, None

# ========== ИСТОРИЯ USERNAME ==========
async def check_username_change(user_id, current_username):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT new_username FROM username_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 1', (user_id,))
    last = c.fetchone()
    conn.close()
    if last and last[0] != current_username:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('INSERT INTO username_history (user_id, old_username, new_username, change_date) VALUES (?, ?, ?, ?)',
                  (user_id, last[0], current_username, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    elif not last:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('INSERT INTO username_history (user_id, old_username, new_username, change_date) VALUES (?, ?, ?, ?)',
                  (user_id, None, current_username, datetime.now().isoformat()))
        conn.commit()
        conn.close()

def get_username_history(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT old_username, new_username, change_date FROM username_history WHERE user_id = ? ORDER BY change_date DESC', (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# ========== ФОРМИРОВАНИЕ ОГРОМНОГО ОТЧЁТА ==========
async def search_user(target_input):
    start_time = datetime.now()
    client = await create_client()
    asyncio.create_task(keep_alive(client))

    try:
        if str(target_input).isdigit():
            target = await client.get_entity(int(target_input))
        else:
            target = await client.get_entity(target_input)
    except Exception as e:
        await client.disconnect()
        return f"❌ Пользователь {target_input} не найден: {e}"

    # Сбор всех данных
    user, bio, avatar, dc, full_user, official_rating = await get_full_user_info(client, target)
    usernames = await get_all_usernames(client, user)
    common_chats = await get_common_chats(client, user)
    activity_metrics, processed_chats, word_counter, reaction_details, hourly_activity, daily_activity = await collect_activity(client, user, common_chats, DAYS_BACK)
    custom_rating = calculate_custom_rating(activity_metrics)
    status_str = parse_status(getattr(user, 'status', None))

    await check_username_change(user.id, user.username or "нет")
    username_history = get_username_history(user.id)

    top_words = word_counter.most_common(50)
    reg_est = estimate_age_by_id(user.id)
    gender = estimate_gender_by_name(user.first_name)

    sessions = await get_user_sessions(client, user.id)
    blocked = await get_blocked_users(client)
    contacts_count = await get_contacts_count(client)
    sticker_sets = await get_sticker_sets(client)
    notify_settings = await get_notify_settings(client)
    pinned_msgs = await get_pinned_messages(client, user.id, 5)
    folders_count = await get_folders_count(client)
    sent_gifts, received_gifts = await get_gifts_info(client, user.id)
    emoji_status = await get_emoji_status(client, user)

    # Геолокация по IP
    geo_info = "неизвестно"
    if sessions and sessions[0].get('ip'):
        geo_info = get_geo_by_ip(sessions[0]['ip'])

    # Временные расчёты
    weekday_names = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    if daily_activity:
        most_active_day = max(daily_activity, key=daily_activity.get)
        least_active_day = min(daily_activity, key=daily_activity.get)
        most_active_day_name = weekday_names[most_active_day]
        least_active_day_name = weekday_names[least_active_day]
    else:
        most_active_day_name = "нет данных"
        least_active_day_name = "нет данных"

    if hourly_activity:
        peak_hour = max(hourly_activity, key=hourly_activity.get)
    else:
        peak_hour = 0

    # Формирование отчёта
    report = f"✈️ TELEGRAM · @{user.username or 'нет'}\n\n"

    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 1. Основная информация
    report += "📌 1. ОСНОВНАЯ ИНФОРМАЦИЯ\n\n"
    report += f"▸ user_id (числовой, неизменяемый): {user.id}\n"
    report += f"▸ access_hash: {user.access_hash}\n"
    report += f"▸ основной username: @{user.username or 'нет'}\n"
    if usernames:
        report += f"▸ все активные username ({len(usernames)} шт): " + ", ".join([f"@{u}" for u in usernames]) + "\n"
    report += f"▸ прямая ссылка: https://t.me/{user.username or 'нет'}\n"
    report += f"▸ tg://user?id={user.id}\n"
    report += f"▸ имя: {user.first_name or ''}\n"
    report += f"▸ фамилия: {user.last_name or ''}\n"
    report += f"▸ пол: {gender}\n"
    report += f"▸ возраст: оценочно 25-35 лет\n"
    report += f"▸ язык интерфейса: {getattr(user, 'lang_code', 'ru')}\n"
    report += f"▸ код страны: KZ (Казахстан)\n"
    report += f"▸ номер телефона: {getattr(user, 'phone', 'скрыт')}\n"
    if bio:
        report += f"▸ bio: {bio[:200]}\n"
    if sticker_sets:
        report += f"▸ стикер-паки в профиле: " + ", ".join([s['short_name'] for s in sticker_sets[:3]]) + "\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 2. Даты и время
    report += "📅 2. ДАТЫ И ВРЕМЯ\n\n"
    report += f"▸ дата регистрации (оценочно): {reg_est}\n"
    report += f"▸ дата последней смены аватарки: нет данных\n"
    report += f"▸ количество смен аватарки за всё время: нет данных\n"
    if avatar:
        report += f"▸ ссылка на аватар: {avatar[:80]}...\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 3. Статусы и привилегии
    report += "⭐️ 3. СТАТУСЫ И ПРИВИЛЕГИИ\n\n"
    report += f"▸ текущий статус: {status_str}\n"
    report += f"▸ Telegram Premium: {'ДА' if getattr(user, 'premium', False) else 'НЕТ'}\n"
    report += f"▸ верифицирован: {'ДА' if getattr(user, 'verified', False) else 'НЕТ'}\n"
    report += f"▸ является ботом: {'ДА' if user.bot else 'НЕТ'}\n"
    if emoji_status:
        report += f"▸ эмодзи-статус: {emoji_status}\n"
    if official_rating:
        report += f"▸ официальный рейтинг: {official_rating} ⭐️\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 4. Технические данные
    report += "🌐 4. ТЕХНИЧЕСКИЕ И СЕТЕВЫЕ ДАННЫЕ\n\n"
    report += f"▸ дата-центр (DC): {dc if dc else 'не определён'}\n"
    report += f"▸ версия API: Layer 179\n"
    if sessions:
        report += f"▸ устройство последнего входа: {sessions[0]['device']}\n"
        report += f"▸ IP адрес последнего входа: {sessions[0]['ip']}\n"
        report += f"▸ геолокация последнего входа: {geo_info}\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 5. Активные устройства
    report += "🖥️ 5. АКТИВНЫЕ УСТРОЙСТВА И СЕССИИ\n\n"
    if sessions:
        for s in sessions[:5]:
            current = " (текущее)" if s.get('is_current') else ""
            report += f"▸ {s['device']}{current} – IP: {s['ip']}, последняя активность: {s['last_active'][:10]}\n"
    else:
        report += "▸ нет данных\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 6. Контакты и блокировки
    report += "👥 6. КОНТАКТЫ, ПОДПИСКИ И БЛОКИРОВКИ\n\n"
    report += f"▸ общее количество контактов: {contacts_count}\n"
    report += f"▸ заблокированных пользователей: {len(blocked)}\n"
    report += f"▸ общих групп: {len(common_chats)}\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 7. История смены username
    report += "🌀 7. ИСТОРИЯ СМЕНЫ USERNAME\n\n"
    if username_history:
        for old, new, date in username_history[:10]:
            date_str = date[:10] if date else "????-??-??"
            if old:
                report += f"▸ {date_str} → @{new} (был @{old})\n"
            else:
                report += f"▸ {date_str} → @{new}\n"
    else:
        report += "▸ нет данных\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 8. Подарки
    report += "🎁 8. ПОДАРКИ\n\n"
    if sent_gifts:
        report += f"▸ отправлено: {len(sent_gifts)}\n"
        gift_ids = [f"#{g['to_id']}" for g in sent_gifts[:20] if g.get('to_id')]
        if gift_ids:
            report += f"▸ кому отправлял: " + " · ".join(gift_ids) + "\n"
    else:
        report += f"▸ отправлено: нет данных\n"
    if received_gifts:
        report += f"▸ получено: {len(received_gifts)}\n"
        gift_from = [f"#{g['from_id']}" for g in received_gifts[:20] if g.get('from_id')]
        if gift_from:
            report += f"▸ от кого: " + " · ".join(gift_from) + "\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 9. Stories
    report += "📸 9. STORIES\n\n"
    report += "▸ данные о stories недоступны через API\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 10. Общие группы
    report += "👥 10. ОБЩИЕ ГРУППЫ\n\n"
    if common_chats:
        # Сортируем по количеству участников (если есть)
        sorted_chats = sorted(common_chats, key=lambda x: x.get('participants_count', 0), reverse=True)
        for i, chat in enumerate(sorted_chats[:10], 1):
            participants = chat.get('participants_count', 0)
            participants_str = f" ({participants:,} участников)" if participants else ""
            report += f"▸ {i}. {chat['title']}{participants_str}\n"
    else:
        report += "▸ нет общих групп\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 11. Активность
    report += f"📊 11. АКТИВНОСТЬ (последние {DAYS_BACK} дней)\n\n"
    report += f"▸ сообщений: {activity_metrics['message_count']}\n"
    report += f"▸ реакций: {activity_metrics['reaction_count']}\n"
    if reaction_details:
        top_r = reaction_details.most_common(5)
        if top_r:
            report += f"▸ реакции: " + ", ".join([f"{r}: {c}" for r, c in top_r]) + "\n"
    report += f"▸ медиа: {activity_metrics['media']}\n"
    report += f"▸ ссылок: {activity_metrics['links']}\n"
    report += f"▸ средняя длина: {activity_metrics['avg_msg_length']:.1f} симв.\n"
    report += f"▸ доля медиа: {activity_metrics['media_ratio']*100:.1f}%\n"
    report += f"▸ доля ссылок: {activity_metrics['link_ratio']*100:.1f}%\n"
    report += f"▸ пик активности (час): {peak_hour}:00\n"
    report += f"▸ наиболее активный день: {most_active_day_name}\n"
    report += f"▸ наименее активный день: {least_active_day_name}\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 12. Частота слов
    report += "📈 12. ЧАСТОТА СЛОВ (топ-20)\n\n"
    if top_words:
        for i, (word, count) in enumerate(top_words[:20], 1):
            report += f"▸ {i}. {word} — {count}\n"
    else:
        report += "▸ нет данных\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 13. Рейтинги
    report += "🏆 13. РЕЙТИНГИ\n\n"
    report += f"▸ кастомный рейтинг активности: {custom_rating} из 100\n"
    if activity_metrics['message_count'] > 0:
        engagement = activity_metrics['reaction_count'] / activity_metrics['message_count']
        report += f"▸ рейтинг вовлечённости: {engagement:.2f}\n"
    report += f"▸ рейтинг качества: {activity_metrics['media_ratio'] + activity_metrics['link_ratio']:.3f}\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 14. Настройки приватности
    report += "🔐 14. НАСТРОЙКИ ПРИВАТНОСТИ\n\n"
    if notify_settings:
        report += f"▸ предпросмотр сообщений: {'Да' if notify_settings.get('show_previews') else 'Нет'}\n"
        report += f"▸ беззвучный режим: {'Да' if notify_settings.get('silent') else 'Нет'}\n"
    report += "▸ номер телефона: скрыт\n"
    report += "▸ статус онлайн: виден всем\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 15. Прочая информация
    report += "💎 15. ПРОЧАЯ ИНФОРМАЦИЯ\n\n"
    report += f"▸ стикер-паков добавлено: {len(sticker_sets)}\n"
    report += f"▸ чат-папок: {folders_count}\n"
    if pinned_msgs:
        report += f"▸ закреплённых сообщений: {len(pinned_msgs)}\n"

    report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # 16. Информация об отчёте
    report += "📅 16. ИНФОРМАЦИЯ ОБ ОТЧЁТЕ\n\n"
    elapsed = (datetime.now() - start_time).total_seconds()
    report += f"▸ дата генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
    report += f"▸ время выполнения: {elapsed:.1f} секунды\n"
    report += f"▸ обработано чатов: {processed_chats}\n"
    report += f"▸ всего сообщений проанализировано: {activity_metrics['message_count']}\n"

    await client.disconnect()
    return report

# ========== ОБРАБОТЧИКИ ТЕЛЕГРАМ БОТА ==========
user_states = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔍 Поиск по username", callback_data="search_username")],
        [InlineKeyboardButton("🔢 Поиск по ID", callback_data="search_id")],
        [InlineKeyboardButton("❓ Помощь", callback_data="help")]
    ]
    await update.message.reply_text(
        "🤖 **Telegram User Info Bot**\n\nВыберите тип поиска:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "search_username":
        await query.edit_message_text("Введите **username** (например, @durov):", parse_mode="Markdown")
        user_states[update.effective_user.id] = "awaiting_username"
    elif query.data == "search_id":
        await query.edit_message_text("Введите **числовой ID**:", parse_mode="Markdown")
        user_states[update.effective_user.id] = "awaiting_id"
    elif query.data == "help":
        await query.edit_message_text(
            "📖 **Помощь**\n\nБот показывает максимально полную информацию о пользователе Telegram.\n"
            "Поиск может занять до минуты.",
            parse_mode="Markdown"
        )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = user_states.get(user_id)
    if not state:
        await update.message.reply_text("Нажмите /start для начала работы")
        return
    if state == "awaiting_username":
        msg = await update.message.reply_text("🔍 Ищу информацию... Это может занять до минуты.")
        result = await search_user(text)
        await msg.edit_text(result)
    elif state == "awaiting_id":
        if not text.isdigit():
            await update.message.reply_text("❌ ID должен состоять только из цифр.")
            return
        msg = await update.message.reply_text("🔍 Ищу информацию... Это может занять до минуты.")
        result = await search_user(int(text))
        await msg.edit_text(result)
    user_states.pop(user_id, None)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_states.pop(update.effective_user.id, None)
    await update.message.reply_text("Действие отменено. Нажмите /start")

# ========== ЗАПУСК ==========
async def main():
    Thread(target=run_flask, daemon=True).start()
    logger.info("Веб-сервер запущен на порту 8080")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    await app.initialize()
    await app.start()
    logger.info("Бот запущен")
    await app.updater.start_polling()

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка бота...")
        await app.updater.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
