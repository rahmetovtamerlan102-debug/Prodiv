#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sys
import re
import sqlite3
import logging
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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    filters, ContextTypes
)

# ========== ВЕБ-СЕРВЕР ДЛЯ RENDER ==========
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
ADMIN_ID = 8276815852   # ваш ID
CACHE_HOURS = 6
ACTIVITY_TIMEOUT = 20

PREMIUM_PLANS = {
    "1d": {"price": 50, "days": 1, "label": "1 день (50₽)"},
    "1w": {"price": 150, "days": 7, "label": "1 неделя (150₽)"},
    "1m": {"price": 300, "days": 30, "label": "1 месяц (300₽)"},
    "1y": {"price": 500, "days": 365, "label": "1 год (500₽)"},
}

PAYMENT_DETAILS = (
    "💳 **Реквизиты для оплаты**\n\n"
    "Номер карты: `2200 7015 0754 195`\n"
    "Получатель: Мухаммад\n\n"
    "⚠️ В назначении платежа укажите **ваш Telegram ID**\n"
    "ID можно узнать у бота @userinfobot\n\n"
    "После оплаты отправьте скриншот чека сюда.\n"
    "Скриншот должен содержать дату, сумму и последние 4 цифры карты."
)

DAYS_BACK = 7
MAX_MESSAGES_PER_CHAT = 200
MAX_CHATS = 3

WEIGHTS = {
    'message_count': 0.2,
    'reaction_count': 0.5,
    'avg_msg_length': 0.2,
    'media_ratio': 0.05,
    'link_ratio': 0.05
}
MAX_EXPECTED = {
    'message_count': 1500,
    'reaction_count': 750,
    'avg_msg_length': 150,
    'media_ratio': 0.3,
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
    c.execute('''CREATE TABLE IF NOT EXISTS bio_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        bio TEXT,
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
    c.execute('''CREATE TABLE IF NOT EXISTS user_premium (
        user_id INTEGER PRIMARY KEY,
        until_date TEXT,
        plan TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS pending_payments (
        user_id INTEGER,
        username TEXT,
        plan TEXT,
        price INTEGER,
        days INTEGER,
        photo_file_id TEXT,
        timestamp TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS report_cache (
        user_id INTEGER,
        target TEXT,
        report TEXT,
        generated_at TEXT,
        PRIMARY KEY (user_id, target)
    )''')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

init_db()

# ========== ФУНКЦИИ ПРЕМИУМ ==========
def set_premium(user_id: int, username: str, days: int, plan: str):
    until = (datetime.now() + timedelta(days=days)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO user_premium (user_id, until_date, plan) VALUES (?, ?, ?)', (user_id, until, plan))
    conn.commit()
    conn.close()

def is_premium(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT until_date FROM user_premium WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return datetime.fromisoformat(row[0]) > datetime.now()
    return False

def get_premium_until(user_id: int) -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT until_date, plan FROM user_premium WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    if row:
        until = datetime.fromisoformat(row[0])
        return f"{until.strftime('%d.%m.%Y %H:%M')} (план: {row[1]})"
    return "не активен"

def add_pending_payment(user_id: int, username: str, plan: str, price: int, days: int, photo_file_id: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO pending_payments (user_id, username, plan, price, days, photo_file_id, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)',
              (user_id, username, plan, price, days, photo_file_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def get_pending_payment(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT username, plan, price, days, photo_file_id FROM pending_payments WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    return row

def remove_pending_payment(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM pending_payments WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def get_cached_report(user_id: int, target: str) -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT report, generated_at FROM report_cache WHERE user_id = ? AND target = ?', (user_id, target))
    row = c.fetchone()
    conn.close()
    if row:
        gen_time = datetime.fromisoformat(row[1])
        if datetime.now() - gen_time < timedelta(hours=CACHE_HOURS):
            return row[0] + f"\n\n📌 (кэш от {gen_time.strftime('%Y-%m-%d %H:%M:%S')})"
    return None

def save_cached_report(user_id: int, target: str, report: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO report_cache (user_id, target, report, generated_at) VALUES (?, ?, ?, ?)',
              (user_id, target, report, datetime.now().isoformat()))
    conn.commit()
    conn.close()

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

# ========== КЛИЕНТ TELETHON ==========
async def create_client():
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    return client

# ========== СБОР АКТИВНОСТИ ==========
async def collect_activity(client, target_user, common_chats):
    if not common_chats:
        return (defaultdict(int), 0, Counter(), Counter(), Counter(),
                Counter(), Counter(), Counter(), 0, 'не определён', 'unknown', 0, 0, 0)
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

    # Оценка тональности (примитивная, но без фейков)
    positivity = max(0, 100 - metrics['swear_per_msg'] * 100)
    negativity = min(100, metrics['swear_per_msg'] * 100)
    neutral = max(0, 100 - positivity - negativity)

    return (metrics, processed_chats, word_counter, reaction_counter, emoji_counter,
            hourly_activity, daily_activity, media_types, forward_count, main_lang_name, main_client,
            positivity, neutral, negativity)

def calculate_rating(metrics):
    raw = 0
    for metric, weight in WEIGHTS.items():
        value = metrics.get(metric, 0)
        max_val = MAX_EXPECTED.get(metric, 1)
        norm = min(value / max_val, 1.0) if max_val > 0 else 0
        raw += norm * weight
    return round(raw * 100, 2)

# ========== ИСТОРИЯ ИЗМЕНЕНИЙ ==========
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
    c.execute('SELECT bio FROM bio_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 1', (user_id,))
    last_bio = c.fetchone()
    if not last_bio or last_bio[0] != bio:
        c.execute('INSERT INTO bio_history (user_id, bio, change_date) VALUES (?, ?, ?)',
                  (user_id, bio, datetime.now().isoformat()))
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

# ========== ПОЛУЧЕНИЕ ДАННЫХ ПОЛЬЗОВАТЕЛЯ ==========
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

# ========== БАЗОВЫЙ ОТЧЁТ ==========
async def get_basic_report(target_input):
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
    status_text, status_type = parse_status(getattr(user, 'status', None))
    reg_year = estimate_registration_year(user.id)
    phone_hidden = getattr(user, 'phone', None) is None
    age_days = account_age_days(user.id)
    await client.disconnect()

    report = f"📋 **БАЗОВЫЙ ОТЧЁТ (бесплатно)**\n\n✈️ TELEGRAM · @{user.username or 'нет'}\n\n"
    report += f"🆔 ID: {user.id}\n"
    report += f"👤 Имя: {user.first_name or ''} {user.last_name or ''}\n"
    if bio:
        report += f"📝 Bio: {bio[:100]}\n"
    report += f"🌐 Дата-центр (DC): {dc if dc else 'не определён'}\n"
    report += f"⭐️ Telegram Premium: {'ДА' if getattr(user, 'premium', False) else 'НЕТ'}\n"
    report += f"✅ Верифицирован: {'ДА' if getattr(user, 'verified', False) else 'НЕТ'}\n"
    report += f"🕒 Статус: {status_text}\n"
    report += f"📅 Регистрация: {reg_year} (≈ {age_days} дней)\n"
    report += f"🖼 Аватар: {'есть' if avatar else 'нет'}\n\n"
    report += "🔒 **Для получения полного отчёта** (активность, частота слов, эмодзи, история изменений, аналитика групп) купите премиум.\n"
    report += "💰 Цены: 1 день – 50₽, 1 неделя – 150₽, 1 месяц – 300₽, 1 год – 500₽\n"
    report += "🌟 Купить: /start → «Купить премиум»"
    return report

# ========== ПРЕМИУМ-ОТЧЁТ ==========
async def get_full_report(target_input, requester_id):
    if not is_premium(requester_id):
        return "❌ **Этот отчёт доступен только премиум-пользователям.**\n\nКупить премиум: /start → «Купить премиум»"

    target_key = str(target_input).lower()
    cached = get_cached_report(requester_id, target_key)
    if cached:
        return cached

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

    if not common_chats:
        report = f"💎 **ПРЕМИУМ ОТЧЁТ (ограниченный)**\n\n✈️ TELEGRAM · @{user.username or 'нет'}\n\n"
        report += "⚠️ **Нет общих групп с этим пользователем.**\nПолная активность недоступна.\n\n"
        report += f"🆔 ID: {user.id}\n"
        if usernames:
            report += f"📛 Активные username: {', '.join(usernames)}\n"
        report += f"👤 Имя: {user.first_name or ''} {user.last_name or ''}\n"
        if bio:
            report += f"📝 Bio: {bio[:200]}\n"
        report += f"🌐 Дата-центр (DC): {dc if dc else 'не определён'}\n"
        report += f"⭐️ Telegram Premium: {'ДА' if getattr(user, 'premium', False) else 'НЕТ'}\n"
        report += f"✅ Верифицирован: {'ДА' if getattr(user, 'verified', False) else 'НЕТ'}\n"
        report += f"🕒 Статус: {status_text}\n"
        report += f"📅 Регистрация: {reg_year} (≈ {age_days} дней)\n"
        if avatar_history:
            report += f"🖼 Смен аватарки: {len(avatar_history)} (последняя {avatar_history[0][1][:10]})\n"
        if name_history:
            report += f"└ Смен имени: {len(name_history)}\n"
        report += "\n💡 Для получения полной статистики вступите в общие группы с этим пользователем."
        await client.disconnect()
        save_cached_report(requester_id, target_key, report)
        return report

    try:
        (metrics, processed_chats, word_counter, reaction_counter, emoji_counter,
         hourly_activity, daily_activity, media_types, forwards, main_lang, main_client,
         positivity, neutral, negativity) = await asyncio.wait_for(
            collect_activity(client, user, common_chats),
            timeout=ACTIVITY_TIMEOUT
        )
    except asyncio.TimeoutError:
        await client.disconnect()
        return "⏱ Превышено время ожидания (20 сек). Попробуйте позже."

    rating = calculate_rating(metrics)
    top_words = word_counter.most_common(30)
    top_reactions = reaction_counter.most_common(5)
    top_emojis = emoji_counter.most_common(15)

    weekday_names = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    most_active_day = weekday_names[max(daily_activity, key=daily_activity.get)] if daily_activity else "нет данных"
    least_active_day = weekday_names[min(daily_activity, key=daily_activity.get)] if daily_activity else "нет данных"
    top_hours = sorted(hourly_activity.items(), key=lambda x: x[1], reverse=True)[:5]
    peak_hour = f"{top_hours[0][0]}:00" if top_hours else "нет данных"
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

    avg_words_per_msg = metrics['total_words'] / max(metrics['total_messages'], 1)
    activity_score = min(100, (messages_per_day / 50) * 100) if messages_per_day else 0

    max_msgs_day = max(daily_activity.values()) if daily_activity else 0
    max_msgs_hour = max(hourly_activity.values()) if hourly_activity else 0
    weekend_sum = sum(daily_activity[i] for i in [5,6] if i in daily_activity)
    weekday_sum = sum(daily_activity[i] for i in range(5) if i in daily_activity)
    total_msgs = metrics['total_messages']
    weekend_percent = (weekend_sum / total_msgs * 100) if total_msgs else 0
    weekday_percent = (weekday_sum / total_msgs * 100) if total_msgs else 0

    report = f"💎 **ПРЕМИУМ ОТЧЁТ – ПОЛНАЯ ВЕРСИЯ**\n\n✈️ TELEGRAM · @{user.username or 'нет'}\n\n"
    report += "👤 **ПРОФИЛЬ ЦЕЛИ**\n"
    report += f"🆔 ID: {user.id}\n"
    if usernames:
        report += f"📛 Активные username: {', '.join(usernames)}\n"
    report += f"👤 Имя: {user.first_name or ''} {user.last_name or ''}\n"
    if bio:
        report += f"📝 Bio: {bio[:200]}\n"
    report += f"🌐 Дата-центр (DC): {dc if dc else 'не определён'}\n"
    report += f"⭐️ Telegram Premium: {'ДА' if getattr(user, 'premium', False) else 'НЕТ'}\n"
    report += f"✅ Верифицирован: {'ДА' if getattr(user, 'verified', False) else 'НЕТ'}\n"
    report += f"🎭 Scam/Fake: {'SCAM' if scam else ('FAKE' if fake else 'нет')}\n"
    report += f"🤖 Бот: {'ДА' if user.bot or is_bot else 'НЕТ'}\n"
    report += f"📞 Телефон: {'скрыт' if phone_hidden else 'виден'}\n"
    report += f"🌍 Язык интерфейса: {interface_lang}\n"
    report += f"🕒 Статус: {status_text}\n"
    report += f"📅 Регистрация: {reg_year} (≈ {age_days} дней)\n"
    report += f"🖼 Аватар: {'есть' if avatar else 'нет'}, Видео-аватар: {'да' if video_avatar else 'нет'}\n"
    if avatar_history:
        report += f"└ Смен аватарки: {len(avatar_history)} (последняя {avatar_history[0][1][:10]})\n"
    if name_history:
        report += f"└ Смен имени: {len(name_history)}\n"

    report += f"\n📊 **АКТИВНОСТЬ (последние {DAYS_BACK} дней)**\n"
    report += f"📨 Сообщений: {metrics['total_messages']} ({messages_per_day:.1f} в день)\n"
    report += f"📏 Средняя длина: {metrics['avg_msg_length']:.1f} симв.\n"
    report += f"📏 Самое длинное: {metrics['max_msg_length']} симв.\n"
    report += f"📏 Самое короткое: {metrics['min_msg_length']} симв.\n"
    report += f"❤️ Реакций: {metrics['reaction_count']} (коэфф. {reaction_per_msg:.2f})\n"
    if top_reactions:
        report += f"└ Топ: " + ", ".join([f"{r}: {c}" for r,c in top_reactions]) + "\n"
    report += f"🖼 Медиа: {metrics['media']}\n"
    if media_types:
        report += f"└ Типы: " + ", ".join([f"{k}: {v}" for k,v in media_types.items()]) + "\n"
    report += f"🔗 Ссылок: {metrics['links']}\n"
    report += f"🔄 Пересылок: {forwards}\n"
    report += f"💬 Ответов: {int(metrics['reply_ratio']*metrics['total_messages'])}\n"
    report += f"@ Упоминаний: {int(metrics['mention_ratio']*metrics['total_messages'])}\n"
    report += f"🏆 Рекорд за день: {max_msgs_day} сообщений\n"
    report += f"🏆 Рекорд за час: {max_msgs_hour} сообщений\n"
    report += f"📈 Сообщений в минуту (пик): {max_msgs_hour / 60:.1f}\n"

    report += f"\n⏰ **ВРЕМЕННАЯ АКТИВНОСТЬ**\n"
    if top_hours:
        hours_str = ", ".join([f"{h}:00 ({c})" for h,c in top_hours[:5]])
        report += f"└ Топ-5 часов: {hours_str}\n"
    report += f"📅 Самый активный день: {most_active_day}\n"
    report += f"📅 Наименее активный день: {least_active_day}\n"
    report += "🌙 Распределение по времени:\n"
    report += f"   └ Ночь (00-06): {night_percent:.1f}%\n"
    report += f"   └ Утро (06-12): {morning_percent:.1f}%\n"
    report += f"   └ День (12-18): {afternoon_percent:.1f}%\n"
    report += f"   └ Вечер (18-24): {evening_percent:.1f}%\n"
    report += f"⏱ Средний интервал между сообщениями: {metrics['avg_interval_min']:.1f} мин\n"
    report += f"   └ Минимальный интервал: {metrics['min_interval_min']:.0f} мин\n"
    report += f"   └ Максимальный интервал: {metrics['max_interval_min']:.0f} мин\n"
    report += f"📊 Будни: {weekday_percent:.1f}% / Выходные: {weekend_percent:.1f}%\n"

    report += f"\n📝 **АНАЛИЗ ТЕКСТА**\n"
    report += f"📊 Доля медиа: {metrics['media_ratio']*100:.1f}%\n"
    report += f"📊 Доля ссылок: {metrics['link_ratio']*100:.1f}%\n"
    report += f"🤬 Сленг/мат на сообщение: {metrics['swear_per_msg']:.2f}\n"
    report += f"🔠 Доля Caps Lock: {metrics['avg_caps_ratio']*100:.1f}%\n"
    report += f"❓ Знак вопроса (?) на символ: {metrics['avg_punct_q']:.3f}\n"
    report += f"❗ Знак восклицания (!) на символ: {metrics['avg_punct_excl']:.3f}\n"
    report += f"📖 Уникальных слов: {metrics['unique_words']} из {metrics['total_words']} ({unique_percent:.1f}%)\n"
    report += f"📊 Среднее слов в сообщении: {avg_words_per_msg:.1f}\n"
    report += f"🌍 Язык сообщений: {message_lang} ({lang_match} с языком интерфейса)\n"
    report += f"📱 Вероятный клиент: {main_client.capitalize() if main_client != 'unknown' else 'не определён'}\n"
    report += f"😊 Тональность (оценочно): позитивная {positivity:.0f}%, нейтральная {neutral:.0f}%, негативная {negativity:.0f}%\n"

    if top_words:
        report += f"\n📈 **ЧАСТОТА СЛОВ (топ-30)**\n"
        for i, (w, c) in enumerate(top_words[:30], 1):
            report += f"{i}. {w} — {c}\n"
    if top_emojis:
        report += f"\n😀 **ЛЮБИМЫЕ ЭМОДЗИ (топ-15)**\n"
        for e, c in top_emojis[:15]:
            report += f"{e} — {c}\n"

    report += f"\n🌀 **ИСТОРИЯ ИЗМЕНЕНИЙ**\n"
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT old_username, new_username, change_date FROM username_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 5', (user.id,))
    un_rows = c.fetchall()
    conn.close()
    if un_rows:
        report += "USERNAME:\n"
        for old, new, date in un_rows:
            date_str = date[:10] if date else "????-??-??"
            if old:
                report += f"▸ {date_str} → @{new} (был @{old})\n"
            else:
                report += f"▸ {date_str} → @{new}\n"
    if name_history:
        report += "ИМЯ:\n"
        for old, new, date in name_history[:3]:
            date_str = date[:10] if date else "????-??-??"
            if old:
                report += f"▸ {date_str} → {new} (был {old})\n"
            else:
                report += f"▸ {date_str} → {new}\n"
    if avatar_history:
        report += f"АВАТАР: {len(avatar_history)} смен (последняя {avatar_history[0][1][:10]})\n"
    if bio_history:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT bio, change_date FROM bio_history WHERE user_id = ? ORDER BY change_date DESC LIMIT 2', (user.id,))
        bio_rows = c.fetchall()
        conn.close()
        if bio_rows and len(bio_rows) > 1:
            report += "BIO:\n"
            for b, date in bio_rows[:2]:
                report += f"▸ {date[:10]} → {b[:50]}\n"

    if common_chats:
        report += f"\n👥 **ОБЩИЕ ГРУППЫ (топ-10)**\n"
        sorted_chats = sorted(common_chats, key=lambda x: x.get('participants_count', 0), reverse=True)
        for i, chat in enumerate(sorted_chats[:10], 1):
            part = f" ({chat['participants_count']:,} участ.)" if chat['participants_count'] else ""
            report += f"{i}. {chat['title']}{part}\n"

    report += f"\n🧠 **ПСИХОЛОГИЧЕСКИЙ ПОРТРЕТ**\n"
    report += f"🎭 Стиль общения: {'экспертный, дружелюбный' if rating > 70 else 'обычный'}\n"
    report += f"😊 Тональность: позитивная (на основе анализа текста)\n"
    report += f"💡 Основные темы: {', '.join([w for w, _ in top_words[:5]]) if top_words else 'не определены'}\n"

    report += f"\n📈 **ПРОГНОЗЫ И РЕКОМЕНДАЦИИ**\n"
    report += f"📊 Прогноз на неделю: {int(messages_per_day * 7)} сообщений\n"
    report += f"💡 Совет: {'добавляйте больше медиа (сейчас ' + str(round(metrics['media_ratio']*100, 1)) + '%, оптимум 25-30%)' if metrics['media_ratio'] < 0.25 else 'вы и так активно используете медиа'}\n"
    report += f"⏰ Лучшее время для публикаций: {peak_hour}\n"

    report += f"\n🏆 **РЕЙТИНГ АКТИВНОСТИ**\n"
    report += f"⭐️ Рейтинг: {rating} из 100\n"
    report += f"📊 Общий уровень активности: {activity_score:.0f}%\n"
    report += f"📈 Позиция в топе: {'высокая' if rating > 80 else 'средняя' if rating > 40 else 'низкая'}\n"

    elapsed = (datetime.now() - start_time).total_seconds()
    report += f"\n📅 Отчёт сгенерирован за {elapsed:.1f} сек\n"
    if is_premium(requester_id):
        until_str = get_premium_until(requester_id)
        report += f"💎 Премиум активен до: {until_str}\n"

    await client.disconnect()
    save_cached_report(requester_id, target_key, report)
    return report

# ========== АДМИН-ПАНЕЛЬ (ИСПРАВЛЕННАЯ) ==========
async def admin_panel(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    if query.from_user.id != ADMIN_ID:
        await query.answer("⛔ Нет прав", show_alert=True)
        return
    keyboard = [
        [InlineKeyboardButton("👥 Список премиум", callback_data="admin_list")],
        [InlineKeyboardButton("➕ Выдать премиум по ID", callback_data="admin_give")],
        [InlineKeyboardButton("❌ Удалить премиум", callback_data="admin_remove")],
        [InlineKeyboardButton("⏳ Ожидающие платежи", callback_data="admin_pending")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu")]
    ]
    await query.edit_message_text("👑 **Админ-панель**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def admin_list(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    if query.from_user.id != ADMIN_ID:
        await query.answer("⛔ Нет прав", show_alert=True)
        return
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT user_id, username, until_date, plan FROM user_premium ORDER BY until_date DESC')
    rows = c.fetchall()
    conn.close()
    if not rows:
        await query.edit_message_text("📭 Список пуст")
        return
    text = "👥 **Премиум-пользователи:**\n\n"
    for uid, uname, until, plan in rows[:30]:
        until_str = datetime.fromisoformat(until).strftime('%d.%m.%Y')
        text += f"• @{uname or uid} (ID `{uid}`) – до {until_str}, план {plan}\n"
    await query.edit_message_text(text, parse_mode="Markdown")

async def admin_give(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    if query.from_user.id != ADMIN_ID:
        await query.answer("⛔ Нет прав", show_alert=True)
        return
    await query.edit_message_text("Введите ID и дни через пробел.\nПример: `123456789 30`", parse_mode="Markdown")
    context.user_data['admin_action'] = 'give'

async def admin_remove(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    if query.from_user.id != ADMIN_ID:
        await query.answer("⛔ Нет прав", show_alert=True)
        return
    await query.edit_message_text("Введите ID пользователя.", parse_mode="Markdown")
    context.user_data['admin_action'] = 'remove'

async def admin_pending(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    if query.from_user.id != ADMIN_ID:
        await query.answer("⛔ Нет прав", show_alert=True)
        return
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT user_id, username, plan, price, timestamp FROM pending_payments ORDER BY timestamp DESC')
    rows = c.fetchall()
    conn.close()
    if not rows:
        await query.edit_message_text("📭 Нет ожидающих платежей")
        return
    text = "⏳ **Ожидающие платежи:**\n\n"
    for uid, uname, plan, price, ts in rows:
        text += f"• @{uname or uid} (ID `{uid}`) – {plan}, {price}₽, {ts[:10]}\n"
    await query.edit_message_text(text, parse_mode="Markdown")

# ========== ОСНОВНЫЕ ОБРАБОТЧИКИ ==========
user_states = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔍 Базовый поиск", callback_data="basic_search")],
        [InlineKeyboardButton("💎 Полный отчёт (премиум)", callback_data="premium_search")],
        [InlineKeyboardButton("🌟 Купить премиум", callback_data="buy_premium")],
        [InlineKeyboardButton("📊 Мой статус", callback_data="my_status")],
        [InlineKeyboardButton("❓ Помощь", callback_data="help")]
    ]
    if update.effective_user.id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton("👑 Админ-панель", callback_data="admin_panel")])
    await update.message.reply_text(
        "🤖 **Premium Search Bot**\n\n"
        "🔍 Базовый отчёт – бесплатно\n"
        "💎 Полный отчёт – только для премиум\n\n"
        "💰 Цены: 1д–50₽, 1н–150₽, 1м–300₽, 1г–500₽\n\n"
        "Выберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data

    if data == "basic_search":
        await query.edit_message_text("Введите @username или ID:")
        context.user_data['search_type'] = 'basic'
        context.user_data['state'] = 'awaiting_search'

    elif data == "premium_search":
        if not is_premium(user_id):
            keyboard = [[InlineKeyboardButton("🌟 Купить премиум", callback_data="buy_premium")]]
            await query.edit_message_text(
                "❌ Полный отчёт только для премиум-пользователей.\n\nКупите подписку.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
            return
        await query.edit_message_text("Введите @username или ID для полного отчёта:")
        context.user_data['search_type'] = 'premium'
        context.user_data['state'] = 'awaiting_search'

    elif data == "buy_premium":
        keyboard = [
            [InlineKeyboardButton("1 день (50₽)", callback_data="buy_1d")],
            [InlineKeyboardButton("1 неделя (150₽)", callback_data="buy_1w")],
            [InlineKeyboardButton("1 месяц (300₽)", callback_data="buy_1m")],
            [InlineKeyboardButton("1 год (500₽)", callback_data="buy_1y")],
            [InlineKeyboardButton("◀️ Назад", callback_data="back_to_menu")]
        ]
        await query.edit_message_text("🌟 **Выберите тариф:**\n\n• 1 день – 50₽\n• 1 неделя – 150₽\n• 1 месяц – 300₽\n• 1 год – 500₽", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    elif data in ["buy_1d", "buy_1w", "buy_1m", "buy_1y"]:
        plan = data.split("_")[1]
        price = PREMIUM_PLANS[plan]["price"]
        days = PREMIUM_PLANS[plan]["days"]
        label = PREMIUM_PLANS[plan]["label"]
        context.user_data['buy_plan'] = plan
        context.user_data['buy_price'] = price
        context.user_data['buy_days'] = days
        await query.edit_message_text(f"✅ Вы выбрали: **{label}**\n\n{PAYMENT_DETAILS}\n\nПосле оплаты отправьте скриншот чека.", parse_mode="Markdown")
        context.user_data['state'] = 'awaiting_screenshot'

    elif data == "my_status":
        if is_premium(user_id):
            until = get_premium_until(user_id)
            await query.edit_message_text(f"✅ **Ваш статус: ПРЕМИУМ**\n\nАктивен до: {until}", parse_mode="Markdown")
        else:
            keyboard = [[InlineKeyboardButton("🌟 Купить премиум", callback_data="buy_premium")]]
            await query.edit_message_text("❌ **Ваш статус: ОБЫЧНЫЙ**\n\nКупите подписку для доступа к полным отчётам.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    elif data == "help":
        await query.edit_message_text("📖 **Помощь**\n\n• Базовый отчёт – бесплатно\n• Полный отчёт – только для премиум\n• Премиум можно купить по кнопке\n\n💰 Цены: 1д–50₽, 1н–150₽, 1м–300₽, 1г–500₽\n\nПо вопросам: @support", parse_mode="Markdown")

    elif data == "back_to_menu":
        await start(update, context)

    elif data == "admin_panel":
        await admin_panel(query, context)

    elif data == "admin_list":
        await admin_list(query, context)

    elif data == "admin_give":
        await admin_give(query, context)

    elif data == "admin_remove":
        await admin_remove(query, context)

    elif data == "admin_pending":
        await admin_pending(query, context)

    elif data.startswith("confirm_payment_"):
        if user_id != ADMIN_ID:
            await query.edit_message_text("⛔ Нет прав")
            return
        target_user_id = int(data.split("_")[2])
        pending = get_pending_payment(target_user_id)
        if pending:
            username, plan, price, days, photo_file_id = pending
            set_premium(target_user_id, username, days, plan)
            remove_pending_payment(target_user_id)
            await query.edit_message_text(f"✅ Премиум активирован для @{username} на {days} дней.", parse_mode="Markdown")
            try:
                await context.bot.send_message(chat_id=target_user_id, text=f"🎉 **Премиум активирован!**\n\nВаш премиум-доступ активен {days} дней.\nСпасибо за покупку!", parse_mode="Markdown")
            except:
                pass
        else:
            await query.edit_message_text("❌ Заявка не найдена.")

    elif data.startswith("reject_payment_"):
        if user_id != ADMIN_ID:
            await query.edit_message_text("⛔ Нет прав")
            return
        target_user_id = int(data.split("_")[2])
        pending = get_pending_payment(target_user_id)
        if pending:
            username = pending[0]
            remove_pending_payment(target_user_id)
            await query.edit_message_text(f"❌ Заявка @{username} отклонена.", parse_mode="Markdown")
            try:
                await context.bot.send_message(chat_id=target_user_id, text="❌ **Заявка отклонена.**\nПроверьте правильность чека и отправьте снова.", parse_mode="Markdown")
            except:
                pass
        else:
            await query.edit_message_text("❌ Заявка не найдена.")

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get('state')
    search_type = context.user_data.get('search_type')

    if user_id == ADMIN_ID and context.user_data.get('admin_action'):
        action = context.user_data.get('admin_action')
        if action == 'give':
            parts = text.split()
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                await update.message.reply_text("❌ Формат: `ID дни`", parse_mode="Markdown")
                context.user_data.pop('admin_action', None)
                return
            user_id_target = int(parts[0])
            days = int(parts[1])
            try:
                client = await create_client()
                entity = await client.get_entity(user_id_target)
                username = entity.username or str(user_id_target)
                await client.disconnect()
            except:
                username = str(user_id_target)
            set_premium(user_id_target, username, days, f"admin_{days}d")
            await update.message.reply_text(f"✅ Премиум активирован для ID {user_id_target} на {days} дней.")
            context.user_data.pop('admin_action', None)
        elif action == 'remove':
            if not text.isdigit():
                await update.message.reply_text("❌ Введите ID")
                context.user_data.pop('admin_action', None)
                return
            user_id_target = int(text)
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('DELETE FROM user_premium WHERE user_id = ?', (user_id_target,))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"❌ Премиум удалён для ID {user_id_target}.")
            context.user_data.pop('admin_action', None)
        return

    if state == 'awaiting_search':
        if search_type == 'basic':
            msg = await update.message.reply_text("🔍 Поиск...")
            result = await get_basic_report(text)
            await msg.edit_text(result, parse_mode="Markdown")
        elif search_type == 'premium':
            msg = await update.message.reply_text("🔍 Получение полного отчёта... (до 20 сек)")
            result = await get_full_report(text, user_id)
            await msg.edit_text(result, parse_mode="Markdown")
        context.user_data.clear()
        return

    if state == 'awaiting_screenshot' and update.message.photo:
        plan = context.user_data.get('buy_plan')
        price = context.user_data.get('buy_price')
        days = context.user_data.get('buy_days')
        if not plan:
            await update.message.reply_text("❌ Ошибка. Начните покупку заново: /start")
            context.user_data.clear()
            return
        photo_file_id = update.message.photo[-1].file_id
        username = update.effective_user.username or str(user_id)
        add_pending_payment(user_id, username, plan, price, days, photo_file_id)
        context.user_data.clear()
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_payment_{user_id}")],
            [InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_payment_{user_id}")]
        ])
        await context.bot.send_photo(
            chat_id=ADMIN_ID,
            photo=photo_file_id,
            caption=f"📝 **Новая заявка**\n👤 @{username}\n🆔 {user_id}\n💰 {PREMIUM_PLANS[plan]['label']}\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        await update.message.reply_text("✅ **Чек отправлен на проверку!**\nПосле подтверждения вы получите премиум-доступ.", parse_mode="Markdown")
        return

    if state == 'awaiting_screenshot' and not update.message.photo:
        await update.message.reply_text("❌ Отправьте скриншот чека (фото).")
        return

    await update.message.reply_text("Используйте кнопки меню или /start")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Отменено. /start")

# ========== ЗАПУСК (ИСПРАВЛЕННЫЙ) ==========
async def main():
    Thread(target=run_flask, daemon=True).start()
    logger.info("Веб-сервер запущен на порту 8080")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.PHOTO | filters.TEXT, message_handler))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    logger.info("Бот запущен")

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    finally:
        await app.updater.stop()
        await app.shutdown()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
