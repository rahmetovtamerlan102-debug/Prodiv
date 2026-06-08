#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sys
import re
import sqlite3
import logging
import math
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from threading import Thread

from flask import Flask
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetCommonChatsRequest
from telethon.tl.types import (
    InputUserSelf, InputUser, UserStatusOnline, UserStatusOffline,
    UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage
)

from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# ========== ВЕБ-СЕРВЕР ==========
flask_app = Flask('')
@flask_app.route('/')
def health():
    return "OK"
def run_flask():
    flask_app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))

# ========== КОНФИГ ==========
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
if not BOT_TOKEN or not API_ID or not API_HASH or not SESSION_STRING:
    print("Ошибка: не заданы BOT_TOKEN, API_ID, API_HASH или SESSION_STRING")
    sys.exit(1)

# ========== НАСТРОЙКИ ==========
DAYS_BACK = 7
MAX_MESSAGES_PER_CHAT = 300
MAX_CHATS = 5

WEIGHTS = {
    'message_count': 0.2,
    'reaction_count': 0.5,
    'avg_msg_length': 0.2,
    'media_ratio': 0.05,
    'link_ratio': 0.05
}
MAX_EXPECTED = {
    'message_count': 500,
    'reaction_count': 250,
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
    c.execute('''CREATE TABLE IF NOT EXISTS premium_history (
        user_id INTEGER PRIMARY KEY,
        is_premium INTEGER,
        last_updated TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS verified_history (
        user_id INTEGER PRIMARY KEY,
        is_verified INTEGER,
        last_updated TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS bio_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        bio TEXT,
        change_date TEXT
    )''')
    conn.commit()
    conn.close()
init_db()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def extract_links(text: str) -> int:
    return len(re.findall(r'https?://[^\s]+', text)) if text else 0

def extract_emojis(text: str) -> list:
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    return emoji_pattern.findall(text)

def dc_from_avatar(avatar_url: str) -> int:
    if not avatar_url:
        return None
    match = re.search(r'cdn(\d)\.', avatar_url)
    if match:
        return {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}.get(int(match.group(1)))
    return None

def parse_status(status) -> tuple:
    if status is None:
        return "скрыт", "скрыт"
    if isinstance(status, UserStatusOnline):
        return "онлайн", "онлайн"
    if isinstance(status, UserStatusOffline):
        if status.was_online:
            return f"был в сети {status.was_online.strftime('%Y-%m-%d %H:%M:%S')}", "точное время"
        return "оффлайн", "оффлайн"
    if isinstance(status, UserStatusRecently):
        return "был недавно", "недавно"
    if isinstance(status, UserStatusLastWeek):
        return "был на этой неделе", "на этой неделе"
    if isinstance(status, UserStatusLastMonth):
        return "был в этом месяце", "в этом месяце"
    return str(status), "другое"

def estimate_registration_year(user_id: int) -> str:
    if user_id < 10000000:
        return "2013–2014"
    elif user_id < 100000000:
        return "2014–2016"
    elif user_id < 500000000:
        return "2016–2018"
    elif user_id < 2000000000:
        return "2018–2021"
    elif user_id < 5000000000:
        return "2021–2024"
    elif user_id < 7000000000:
        return "2024–2025"
    else:
        return "2025–2026"

def account_age_days(user_id: int) -> int:
    if user_id < 100000:
        days = 0
    elif user_id < 1000000:
        days = 90
    elif user_id < 10000000:
        days = 180
    elif user_id < 100000000:
        days = 365
    elif user_id < 500000000:
        days = 730
    elif user_id < 2000000000:
        days = 1095
    elif user_id < 5000000000:
        days = 1460
    else:
        days = 1825
    return days

def has_emoji(text: str) -> bool:
    return bool(extract_emojis(text)) if text else False

def has_link(text: str) -> bool:
    return bool(re.search(r'https?://', text)) if text else False

def detect_swear(text: str) -> int:
    swear_words = ['бля', 'хуй', 'пизд', 'еба', 'заеб', 'мудак', 'гандон', 'сука', 'пидор', 'уёбок']
    count = 0
    low = text.lower()
    for w in swear_words:
        count += low.count(w)
    return count

def caps_ratio(text: str) -> float:
    if not text:
        return 0
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return 0
    upper = sum(1 for c in letters if c.isupper())
    return upper / len(letters)

def punct_ratio(text: str, punct: str) -> float:
    if not text:
        return 0
    return text.count(punct) / len(text)

def get_language(text: str) -> str:
    try:
        return detect(text)
    except:
        return 'unknown'

def guess_client(msg) -> str:
    if msg.media and isinstance(msg.media, MessageMediaDocument):
        mime = getattr(msg.media.document, 'mime_type', '')
        if 'round' in mime:
            return 'mobile'
    if msg.media and isinstance(msg.media, MessageMediaPhoto):
        return 'mobile'
    return 'unknown'

# ========== КЛИЕНТ ==========
async def create_client():
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    return client

# ========== СБОР АКТИВНОСТИ ==========
async def collect_activity(client, target_user, common_chats):
    metrics = defaultdict(int)
    word_counter = Counter()
    reaction_counter = Counter()
    emoji_counter = Counter()
    media_types = Counter()
    msg_lengths = []
    hourly_activity = Counter()
    daily_activity = Counter()
    swear_count = 0
    caps_sum = 0
    punct_q = 0
    punct_excl = 0
    punct_dots = 0
    reply_count = 0
    mention_count = 0
    forward_count = 0
    cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
    processed_chats = 0
    all_msg_times = []
    language_counter = Counter()
    client_counter = Counter()

    for chat in common_chats[:MAX_CHATS]:
        chat_id = chat['id']
        try:
            async for msg in client.iter_messages(chat_id, from_user=target_user.id, offset_date=cutoff_date, limit=MAX_MESSAGES_PER_CHAT):
                metrics['total_messages'] += 1
                all_msg_times.append(msg.date)
                cl = guess_client(msg)
                client_counter[cl] += 1
                if msg.text:
                    text = msg.text
                    metrics['total_chars'] += len(text)
                    metrics['links'] += extract_links(text)
                    words = re.findall(r'\b[а-яА-Яa-zA-Z]{3,}\b', text.lower())
                    word_counter.update(words)
                    emojis = extract_emojis(text)
                    emoji_counter.update(emojis)
                    msg_lengths.append(len(text))
                    swear_count += detect_swear(text)
                    caps_sum += caps_ratio(text)
                    punct_q += punct_ratio(text, '?')
                    punct_excl += punct_ratio(text, '!')
                    punct_dots += punct_ratio(text, '.')
                    if text.startswith('@'):
                        mention_count += 1
                    if re.search(r'@\w+', text):
                        mention_count += 1
                    lang = get_language(text)
                    if lang != 'unknown':
                        language_counter[lang] += 1
                if msg.media:
                    metrics['media'] += 1
                    if isinstance(msg.media, MessageMediaPhoto):
                        media_types['фото'] += 1
                    elif isinstance(msg.media, MessageMediaDocument):
                        mime = getattr(msg.media.document, 'mime_type', '')
                        if 'video' in mime:
                            media_types['видео'] += 1
                        elif 'gif' in mime:
                            media_types['gif'] += 1
                        elif 'voice' in mime:
                            media_types['голосовое'] += 1
                        else:
                            media_types['файл'] += 1
                    elif hasattr(msg.media, 'webpage'):
                        media_types['ссылка-превью'] += 1
                if msg.reactions:
                    for r in msg.reactions.results:
                        metrics['reactions'] += r.count
                        if hasattr(r, 'reaction') and hasattr(r.reaction, 'emoticon'):
                            reaction_counter[r.reaction.emoticon] += r.count
                if msg.fwd_from:
                    forward_count += 1
                if msg.reply_to_msg_id:
                    reply_count += 1
                if msg.date:
                    hour = msg.date.hour
                    weekday = msg.date.weekday()
                    hourly_activity[hour] += 1
                    daily_activity[weekday] += 1
            processed_chats += 1
            await asyncio.sleep(0.2)
        except errors.FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 3))
        except Exception:
            pass

    if metrics['total_messages'] > 0:
        metrics['avg_msg_length'] = metrics['total_chars'] / metrics['total_messages']
        metrics['media_ratio'] = metrics['media'] / metrics['total_messages']
        metrics['link_ratio'] = metrics['links'] / metrics['total_messages']
        metrics['max_msg_length'] = max(msg_lengths)
        metrics['min_msg_length'] = min(msg_lengths)
        metrics['unique_words'] = len(word_counter)
        metrics['total_words'] = sum(word_counter.values())
        metrics['swear_per_msg'] = swear_count / metrics['total_messages']
        metrics['avg_caps_ratio'] = caps_sum / metrics['total_messages']
        metrics['avg_punct_q'] = punct_q / metrics['total_messages']
        metrics['avg_punct_excl'] = punct_excl / metrics['total_messages']
        metrics['avg_punct_dots'] = punct_dots / metrics['total_messages']
        metrics['reply_ratio'] = reply_count / metrics['total_messages']
        metrics['mention_ratio'] = mention_count / metrics['total_messages']
        metrics['forward_ratio'] = forward_count / metrics['total_messages']
        if len(all_msg_times) > 1:
            all_msg_times.sort()
            diffs = [(all_msg_times[i+1] - all_msg_times[i]).total_seconds() / 60 for i in range(len(all_msg_times)-1)]
            metrics['avg_interval_min'] = sum(diffs) / len(diffs)
            metrics['max_interval_min'] = max(diffs)
            metrics['min_interval_min'] = min(diffs)
        else:
            metrics['avg_interval_min'] = metrics['max_interval_min'] = metrics['min_interval_min'] = 0
    else:
        metrics['avg_msg_length'] = 0
        metrics['media_ratio'] = 0
        metrics['link_ratio'] = 0
        metrics['max_msg_length'] = 0
        metrics['min_msg_length'] = 0
        metrics['unique_words'] = 0
        metrics['total_words'] = 0
        metrics['swear_per_msg'] = 0
        metrics['avg_caps_ratio'] = 0
        metrics['avg_punct_q'] = 0
        metrics['avg_punct_excl'] = 0
        metrics['avg_punct_dots'] = 0
        metrics['reply_ratio'] = 0
        metrics['mention_ratio'] = 0
        metrics['forward_ratio'] = 0
        metrics['avg_interval_min'] = metrics['max_interval_min'] = metrics['min_interval_min'] = 0

    most_common_lang = language_counter.most_common(1)
    main_lang = most_common_lang[0][0] if most_common_lang else 'unknown'
    most_common_client = client_counter.most_common(1)
    main_client = most_common_client[0][0] if most_common_client else 'unknown'
    lang_names = {
        'ru': 'русский', 'en': 'английский', 'uk': 'украинский', 'be': 'белорусский',
        'kk': 'казахский', 'de': 'немецкий', 'fr': 'французский', 'es': 'испанский',
        'it': 'итальянский', 'tr': 'турецкий', 'zh-cn': 'китайский', 'ja': 'японский',
        'unknown': 'не определён'
    }
    main_lang_name = lang_names.get(main_lang, main_lang)

    return (metrics, processed_chats, word_counter, reaction_counter, emoji_counter,
            hourly_activity, daily_activity, media_types, forward_count, main_lang_name, main_client)

def calculate_rating(metrics):
    raw = 0
    for metric, weight in WEIGHTS.items():
        value = metrics.get(metric, 0)
        max_val = MAX_EXPECTED.get(metric, 1)
        norm = min(value / max_val, 1.0) if max_val > 0 else 0
        raw += norm * weight
    return round(raw * 100, 2)

# ========== ИСТОРИЯ ==========
async def update_history(user_id, current_username, current_first_name, current_last_name, avatar_hash, is_premium, is_verified, bio):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT new_username FROM username_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 1', (user_id,))
    last_username = c.fetchone()
    if not last_username or last_username[0] != current_username:
        c.execute('INSERT INTO username_history (user_id, old_username, new_username, change_date) VALUES (?, ?, ?, ?)',
                  (user_id, last_username[0] if last_username else None, current_username, datetime.now().isoformat()))
    full_name = f"{current_first_name or ''} {current_last_name or ''}".strip()
    c.execute('SELECT new_name FROM name_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 1', (user_id,))
    last_name = c.fetchone()
    if not last_name or last_name[0] != full_name:
        c.execute('INSERT INTO name_history (user_id, old_name, new_name, change_date) VALUES (?, ?, ?, ?)',
                  (user_id, last_name[0] if last_name else None, full_name, datetime.now().isoformat()))
    c.execute('SELECT avatar_hash FROM avatar_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 1', (user_id,))
    last_avatar = c.fetchone()
    if not last_avatar or last_avatar[0] != avatar_hash:
        c.execute('INSERT INTO avatar_history (user_id, avatar_hash, change_date) VALUES (?, ?, ?)',
                  (user_id, avatar_hash, datetime.now().isoformat()))
    c.execute('SELECT is_premium FROM premium_history WHERE user_id = ?', (user_id,))
    prem = c.fetchone()
    if not prem or prem[0] != is_premium:
        c.execute('INSERT OR REPLACE INTO premium_history (user_id, is_premium, last_updated) VALUES (?, ?, ?)',
                  (user_id, is_premium, datetime.now().isoformat()))
    c.execute('SELECT is_verified FROM verified_history WHERE user_id = ?', (user_id,))
    ver = c.fetchone()
    if not ver or ver[0] != is_verified:
        c.execute('INSERT OR REPLACE INTO verified_history (user_id, is_verified, last_updated) VALUES (?, ?, ?)',
                  (user_id, is_verified, datetime.now().isoformat()))
    c.execute('SELECT bio FROM bio_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 1', (user_id,))
    last_bio = c.fetchone()
    if not last_bio or last_bio[0] != bio:
        c.execute('INSERT INTO bio_history (user_id, bio, change_date) VALUES (?, ?, ?)',
                  (user_id, bio, datetime.now().isoformat()))
    conn.commit()
    conn.close()

async def get_name_history(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT old_name, new_name, change_date FROM name_history WHERE user_id = ? ORDER BY change_date DESC', (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows

async def get_avatar_history(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT avatar_hash, change_date FROM avatar_history WHERE user_id = ? ORDER BY change_date DESC', (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows

async def get_premium_history(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT is_premium, last_updated FROM premium_history WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    return row

async def get_verified_history(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT is_verified, last_updated FROM verified_history WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    return row

# ========== ПОЛУЧЕНИЕ ДАННЫХ ==========
async def get_user_full_info(client, target):
    try:
        full = await client(GetFullUserRequest(target))
        user = full.users[0]
        full_user = full.full_user
        bio = getattr(full_user, 'about', None)
        avatar = None
        video_avatar = False
        if full_user.photo:
            if hasattr(full_user.photo, 'sizes') and full_user.photo.sizes:
                avatar = str(full_user.photo.sizes[-1].location)
            if hasattr(full_user.photo, 'video_sizes') and full_user.photo.video_sizes:
                video_avatar = True
        dc = None
        if hasattr(user, 'photo') and user.photo and hasattr(user.photo, 'dc_id'):
            dc = user.photo.dc_id
        if not dc and avatar:
            dc = dc_from_avatar(avatar)
        return (user, bio, avatar, video_avatar, dc, getattr(user, 'lang_code', None),
                getattr(user, 'scam', False), getattr(user, 'fake', False), getattr(full_user, 'bot_info', None) is not None)
    except Exception as e:
        logger.error(f"Full info error: {e}")
        return target, None, None, False, None, None, False, False, False

async def get_common_chats(client, target_user):
    try:
        common = await client(GetCommonChatsRequest(
            user_id=InputUser(target_user.id, target_user.access_hash),
            max_id=0,
            limit=100
        ))
        chats = []
        for c in common.chats:
            chats.append({
                'id': c.id,
                'title': getattr(c, 'title', str(c.id)),
                'participants_count': getattr(c, 'participants_count', 0)
            })
        return chats
    except Exception as e:
        logger.error(f"Ошибка общих групп: {e}")
        return []

# ========== ПОИСК ==========
async def search_user(target_input):
    start_time = datetime.now()
    client = await create_client()
    try:
        if str(target_input).isdigit():
            target = await client.get_entity(int(target_input))
        else:
            target = await client.get_entity(target_input)
    except Exception as e:
        await client.disconnect()
        return f"❌ Пользователь {target_input} не найден: {e}"
    user, bio, avatar, video_avatar, dc, lang, scam, fake, is_bot = await get_user_full_info(client, target)
    usernames = [user.username] if user.username else []
    if hasattr(user, 'usernames') and user.usernames:
        for u in user.usernames:
            if u.username and u.username != user.username:
                usernames.append(u.username)
    common_chats = await get_common_chats(client, user)
    status_text, status_type = parse_status(getattr(user, 'status', None))
    reg_year = estimate_registration_year(user.id)
    phone_hidden = getattr(user, 'phone', None) is None
    age_days = account_age_days(user.id)

    avatar_hash = avatar if avatar else ''
    await update_history(user.id, user.username or '', user.first_name or '', user.last_name or '', avatar_hash,
                         getattr(user, 'premium', False), getattr(user, 'verified', False), bio or '')
    name_history = await get_name_history(user.id)
    avatar_history = await get_avatar_history(user.id)
    premium_history = await get_premium_history(user.id)
    verified_history = await get_verified_history(user.id)

    (metrics, processed_chats, word_counter, reaction_counter, emoji_counter,
     hourly_activity, daily_activity, media_types, forwards, main_lang, main_client) = await collect_activity(client, user, common_chats)
    rating = calculate_rating(metrics)

    top_words = word_counter.most_common(20)
    top_reactions = reaction_counter.most_common(5)
    top_emojis = emoji_counter.most_common(10)

    weekday_names = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    most_active_day = weekday_names[max(daily_activity, key=daily_activity.get)] if daily_activity else "нет данных"
    least_active_day = weekday_names[min(daily_activity, key=daily_activity.get)] if daily_activity else "нет данных"
    peak_hour = f"{max(hourly_activity, key=hourly_activity.get)}:00" if hourly_activity else "нет данных"
    night_activity = sum(v for h,v in hourly_activity.items() if h>=0 and h<6)
    morning_activity = sum(v for h,v in hourly_activity.items() if h>=6 and h<12)
    afternoon_activity = sum(v for h,v in hourly_activity.items() if h>=12 and h<18)
    evening_activity = sum(v for h,v in hourly_activity.items() if h>=18 and h<24)
    total_activity = night_activity + morning_activity + afternoon_activity + evening_activity
    night_percent = (night_activity / total_activity * 100) if total_activity else 0
    morning_percent = (morning_activity / total_activity * 100) if total_activity else 0
    afternoon_percent = (afternoon_activity / total_activity * 100) if total_activity else 0
    evening_percent = (evening_activity / total_activity * 100) if total_activity else 0

    messages_per_day = metrics['total_messages'] / DAYS_BACK if DAYS_BACK > 0 else 0
    reaction_per_msg = metrics['reaction_count'] / metrics['total_messages'] if metrics['total_messages'] else 0
    unique_percent = (metrics['unique_words'] / max(metrics['total_words'], 1) * 100)

    interface_lang = lang if lang else 'не определён'
    message_lang = main_lang
    lang_match = "совпадает" if (interface_lang != 'не определён' and interface_lang[:2] == message_lang[:2]) else "отличается"
    if interface_lang == 'не определён' or message_lang == 'не определён':
        lang_match = "нельзя сравнить"

    # Дополнительные метрики
    avg_words_per_msg = metrics['total_words'] / max(metrics['total_messages'], 1)
    activity_score = min(100, (messages_per_day / 50) * 100) if messages_per_day else 0

    report = f"✈️ TELEGRAM · @{user.username or 'нет'}\n\n"
    report += "╔══════════════════════════════════════════════════════════════════╗\n"
    report += "║                         👤 ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ                    ║\n"
    report += "╚══════════════════════════════════════════════════════════════════╝\n\n"

    report += f"▸ ID: {user.id}\n"
    if usernames:
        report += f"▸ Активные username: " + ", ".join(usernames) + "\n"
    report += f"▸ Имя: {user.first_name or ''} {user.last_name or ''}\n"
    if bio:
        report += f"▸ Bio: {bio[:200]}\n"
    report += f"▸ Дата-центр (DC): {dc if dc else 'не определён'}\n"
    report += f"▸ Telegram Premium: {'ДА' if getattr(user, 'premium', False) else 'НЕТ'}\n"
    if premium_history:
        report += f"   └ Премиум зафиксирован с {premium_history[1][:10]}\n"
    report += f"▸ Верифицирован: {'ДА' if getattr(user, 'verified', False) else 'НЕТ'}\n"
    if verified_history:
        report += f"   └ Верификация зафиксирована с {verified_history[1][:10]}\n"
    report += f"▸ Scam/Fake метка: {'SCAM' if scam else ('FAKE' if fake else 'нет')}\n"
    report += f"▸ Бот: {'ДА' if user.bot or is_bot else 'НЕТ'}\n"
    report += f"▸ Номер телефона: {'скрыт' if phone_hidden else 'виден'}\n"
    report += f"▸ Язык интерфейса: {interface_lang}\n"
    report += f"▸ Язык сообщений: {message_lang} ({lang_match} с языком интерфейса)\n"
    report += f"▸ Вероятный клиент: {main_client.capitalize() if main_client != 'unknown' else 'не определён'}\n"
    report += f"▸ Статус: {status_text}\n"
    report += f"   └ Видимость: {status_type}\n"
    report += f"▸ Регистрация (оценочно): {reg_year}\n"
    report += f"   └ Возраст аккаунта: ≈ {age_days} дней\n"
    report += f"▸ Аватар: {'есть' if avatar else 'нет'}, Видео-аватар: {'да' if video_avatar else 'нет'}\n"
    if avatar_history:
        report += f"   └ Смен аватарки: {len(avatar_history)} (последняя {avatar_history[0][1][:10]})\n"
    if name_history:
        report += f"   └ Смен имени: {len(name_history)}\n"

    report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
    report += f"║                         👥 ОБЩИЕ ГРУППЫ                           ║\n"
    report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
    report += f"▸ Всего общих групп: {len(common_chats)}\n"
    if common_chats:
        top_chats = sorted(common_chats, key=lambda x: x.get('participants_count', 0), reverse=True)[:10]
        report += "▸ Топ-10 по количеству участников:\n"
        for i, c in enumerate(top_chats, 1):
            part = f" ({c['participants_count']:,} участ.)" if c['participants_count'] else ""
            report += f"   {i}. {c['title']}{part}\n"

    report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
    report += f"║                       🌀 ИСТОРИЯ USERNAME                         ║\n"
    report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT old_username, new_username, change_date FROM username_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 10', (user.id,))
    username_rows = c.fetchall()
    conn.close()
    if username_rows:
        for old, new, date in username_rows:
            date_str = date[:10] if date else "????-??-??"
            if old:
                report += f"▸ {date_str} → @{new} (был @{old})\n"
            else:
                report += f"▸ {date_str} → @{new}\n"
    else:
        report += "▸ нет данных (накопление начнётся после первого поиска)\n"

    report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
    report += f"║                        📊 СТАТИСТИКА АКТИВНОСТИ                    ║\n"
    report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
    report += f"▸ Период анализа: последние {DAYS_BACK} дней\n"
    report += f"▸ Всего сообщений: {metrics['total_messages']}\n"
    report += f"▸ Сообщений в день: {messages_per_day:.1f}\n"
    report += f"▸ Всего символов: {metrics['total_chars']:,}\n"
    report += f"▸ Средняя длина сообщения: {metrics['avg_msg_length']:.1f} симв.\n"
    report += f"▸ Самое длинное сообщение: {metrics['max_msg_length']} симв.\n"
    report += f"▸ Самое короткое сообщение: {metrics['min_msg_length']} симв.\n"
    report += f"▸ Среднее количество слов в сообщении: {avg_words_per_msg:.1f}\n"
    report += f"▸ Всего реакций получено: {metrics['reaction_count']}\n"
    report += f"▸ Коэффициент реакций на сообщение: {reaction_per_msg:.2f}\n"
    if top_reactions:
        report += f"▸ Топ реакций:\n"
        for r, c in top_reactions:
            report += f"   └ {r}: {c}\n"
    report += f"▸ Всего медиа: {metrics['media']}\n"
    if media_types:
        report += f"▸ Типы медиа:\n"
        for k, v in media_types.items():
            report += f"   └ {k}: {v}\n"
    report += f"▸ Ссылок: {metrics['links']}\n"
    report += f"▸ Пересылок (форвардов): {forwards}\n"
    report += f"▸ Ответов на сообщения: {int(metrics['reply_ratio']*metrics['total_messages'])}\n"
    report += f"▸ Упоминаний (@): {int(metrics['mention_ratio']*metrics['total_messages'])}\n"

    report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
    report += f"║                      ⏰ ВРЕМЕННАЯ АКТИВНОСТЬ                      ║\n"
    report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
    report += f"▸ Пик активности: {peak_hour}\n"
    report += f"▸ Самый активный день: {most_active_day}\n"
    report += f"▸ Наименее активный день: {least_active_day}\n"
    report += f"▸ Распределение по времени суток:\n"
    report += f"   └ Ночь (00-06): {night_percent:.1f}%\n"
    report += f"   └ Утро (06-12): {morning_percent:.1f}%\n"
    report += f"   └ День (12-18): {afternoon_percent:.1f}%\n"
    report += f"   └ Вечер (18-24): {evening_percent:.1f}%\n"
    report += f"▸ Средний интервал между сообщениями: {metrics['avg_interval_min']:.1f} мин\n"
    report += f"   └ Минимальный интервал: {metrics['min_interval_min']:.0f} мин\n"
    report += f"   └ Максимальный интервал: {metrics['max_interval_min']:.0f} мин\n"

    report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
    report += f"║                      📝 АНАЛИЗ ТЕКСТА                           ║\n"
    report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
    report += f"▸ Доля медиа в сообщениях: {metrics['media_ratio']*100:.1f}%\n"
    report += f"▸ Доля ссылок в сообщениях: {metrics['link_ratio']*100:.1f}%\n"
    report += f"▸ Сленг/мат на сообщение: {metrics['swear_per_msg']:.2f}\n"
    report += f"▸ Доля Caps Lock: {metrics['avg_caps_ratio']*100:.1f}%\n"
    report += f"▸ Знак вопроса (?) на символ: {metrics['avg_punct_q']:.3f}\n"
    report += f"▸ Знак восклицания (!) на символ: {metrics['avg_punct_excl']:.3f}\n"
    report += f"▸ Многоточие (...) на символ: {metrics['avg_punct_dots']:.3f}\n"
    report += f"▸ Уникальных слов: {metrics['unique_words']} из {metrics['total_words']}\n"
    report += f"▸ Уникальность лексики: {unique_percent:.1f}%\n"

    if top_words:
        report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
        report += f"║                      📈 ЧАСТОТА СЛОВ (топ-20)                    ║\n"
        report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
        for i, (w, c) in enumerate(top_words, 1):
            report += f"▸ {i}. {w} — {c}\n"

    if top_emojis:
        report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
        report += f"║                      😀 ЛЮБИМЫЕ ЭМОДЗИ                          ║\n"
        report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
        for e, c in top_emojis:
            report += f"▸ {e} — {c}\n"

    report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
    report += f"║                      🏆 РЕЙТИНГИ                                 ║\n"
    report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
    report += f"▸ Рейтинг активности: {rating} из 100\n"
    report += f"▸ Общий уровень активности: {activity_score:.0f}%\n"

    elapsed = (datetime.now() - start_time).total_seconds()
    report += f"\n╔══════════════════════════════════════════════════════════════════╗\n"
    report += f"║                      📅 ИНФОРМАЦИЯ ОБ ОТЧЁТЕ                      ║\n"
    report += f"╚══════════════════════════════════════════════════════════════════╝\n\n"
    report += f"▸ Время генерации: {elapsed:.1f} сек\n"
    report += f"▸ Обработано чатов: {processed_chats}\n"
    report += f"▸ Проанализировано сообщений: {metrics['total_messages']}\n"
    report += f"▸ Дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC\n"

    await client.disconnect()
    return report

# ========== ОБРАБОТЧИКИ БОТА ==========
user_states = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔍 Поиск по username", callback_data="search_username")],
        [InlineKeyboardButton("🔢 Поиск по ID", callback_data="search_id")],
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
        await query.edit_message_text("Введите @username:", parse_mode="Markdown")
        user_states[update.effective_user.id] = "awaiting_username"
    elif query.data == "search_id":
        await query.edit_message_text("Введите числовой ID:", parse_mode="Markdown")
        user_states[update.effective_user.id] = "awaiting_id"

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text.strip()
    state = user_states.get(uid)
    if not state:
        await update.message.reply_text("Нажмите /start")
        return
    if state == "awaiting_username":
        msg = await update.message.reply_text("🔍 Сбор данных... (до 20 сек)")
        result = await search_user(text)
        await msg.edit_text(result)
    elif state == "awaiting_id":
        if not text.isdigit():
            await update.message.reply_text("❌ ID только цифры")
            return
        msg = await update.message.reply_text("🔍 Сбор данных... (до 20 сек)")
        result = await search_user(int(text))
        await msg.edit_text(result)
    user_states.pop(uid, None)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_states.pop(update.effective_user.id, None)
    await update.message.reply_text("Отменено. /start")

# ========== ЗАПУСК ==========
async def main():
    Thread(target=run_flask, daemon=True).start()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    try:
        while True:
            await asyncio.sleep(3600)
    except:
        await app.updater.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
