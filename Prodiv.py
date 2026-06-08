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
from telethon.tl.functions.account import GetAuthorizationsRequest
from telethon.tl.functions.contacts import GetBlockedRequest, GetContactsRequest
from telethon.tl.types import (
    InputUserSelf, InputPeerUser, UserStatusOnline, UserStatusOffline
)

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
DAYS_BACK = 7
MAX_MESSAGES_PER_CHAT = 200
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
    conn.commit()
    conn.close()

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

# ========== БЫСТРЫЙ СБОР ДАННЫХ ==========
async def get_user_sessions(client):
    try:
        result = await client(GetAuthorizationsRequest())
        sessions = []
        for auth in result.authorizations[:3]:
            sessions.append({
                'device': f"{auth.device_model} ({auth.app_name})",
                'ip': auth.ip,
                'last_active': auth.date_active.isoformat()[:10],
                'is_current': auth.current
            })
        return sessions
    except:
        return []

async def get_blocked_count(client):
    try:
        result = await client(GetBlockedRequest(offset=0, limit=1))
        return result.count if hasattr(result, 'count') else len(result.blocked)
    except:
        return 0

async def get_contacts_count(client):
    try:
        result = await client(GetContactsRequest(hash=0))
        return len(result.users) if result.users else 0
    except:
        return 0

async def collect_activity_fast(client, target_user, common_chats, days_back=7):
    metrics = defaultdict(int)
    word_counter = Counter()
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
                if msg.reactions:
                    for r in msg.reactions.results:
                        metrics['reactions'] += r.count
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
    else:
        metrics['avg_msg_length'] = 0
        metrics['media_ratio'] = 0
        metrics['link_ratio'] = 0

    return metrics, processed_chats, word_counter

def calculate_rating(metrics):
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
        return user, bio, avatar, dc
    except Exception as e:
        logger.error(f"Ошибка full info: {e}")
        return target, None, None, None

async def get_common_chats(client):
    try:
        common = await client(GetCommonChatsRequest(
            user_id=InputUserSelf(),
            max_id=0,
            limit=50
        ))
        chats = []
        for c in common.chats:
            chats.append({
                'id': c.id,
                'title': getattr(c, 'title', str(c.id)),
                'participants_count': getattr(c, 'participants_count', 0)
            })
        return chats
    except:
        return []

async def get_all_usernames(client, target_user):
    usernames = [target_user.username] if target_user.username else []
    if hasattr(target_user, 'usernames') and target_user.usernames:
        for un in target_user.usernames:
            if un.username and un.username != target_user.username:
                usernames.append(un.username)
    return usernames

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

# ========== БЫСТРЫЙ ПОИСК ==========
async def search_user_fast(target_input):
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

    # Основная информация (быстро)
    user, bio, avatar, dc = await get_full_user_info(client, target)
    usernames = await get_all_usernames(client, user)
    status_str = parse_status(getattr(user, 'status', None))
    reg_est = estimate_age_by_id(user.id)

    # Общие группы
    common_chats = await get_common_chats(client)

    # Активность (быстро, с ограничениями)
    activity_metrics, processed_chats, word_counter = await collect_activity_fast(client, user, common_chats, DAYS_BACK)
    rating = calculate_rating(activity_metrics)

    await check_username_change(user.id, user.username or "нет")
    username_history = get_username_history(user.id)

    # Сессии
    sessions = await get_user_sessions(client)
    contacts_count = await get_contacts_count(client)
    blocked_count = await get_blocked_count(client)

    top_words = word_counter.most_common(10)

    # Компактный отчёт
    report = f"✈️ TELEGRAM · @{user.username or 'нет'}\n\n"
    report += f"🆔 ID: {user.id}\n"
    if usernames:
        report += f"📛 Username: " + ", ".join([f"@{u}" for u in usernames]) + "\n"
    report += f"👤 Имя: {user.first_name or ''} {user.last_name or ''}\n"
    if bio:
        report += f"📝 Bio: {bio[:100]}\n"
    report += f"🌐 Дата-центр (DC): {dc if dc else 'не определён'}\n"
    report += f"⭐️ Telegram Premium: {'ДА' if getattr(user, 'premium', False) else 'НЕТ'}\n"
    report += f"✅ Верифицирован: {'ДА' if getattr(user, 'verified', False) else 'НЕТ'}\n"
    report += f"🕒 Статус: {status_str}\n"
    report += f"📅 Регистрация (оценочно): {reg_est}\n"

    report += f"\n🖥️ УСТРОЙСТВА:\n"
    if sessions:
        for s in sessions[:3]:
            current = " (текущее)" if s.get('is_current') else ""
            report += f"▸ {s['device']}{current} – {s['ip']}, {s['last_active']}\n"
    else:
        report += "▸ нет данных\n"

    report += f"\n👥 КОНТАКТЫ И ГРУППЫ:\n"
    report += f"▸ Контактов: {contacts_count}\n"
    report += f"▸ Заблокировано: {blocked_count}\n"
    report += f"▸ Общих групп: {len(common_chats)}\n"

    report += f"\n🌀 ИСТОРИЯ USERNAME:\n"
    if username_history:
        for old, new, date in username_history[:5]:
            date_str = date[:10] if date else "????-??-??"
            if old:
                report += f"▸ {date_str} → @{new} (был @{old})\n"
            else:
                report += f"▸ {date_str} → @{new}\n"
    else:
        report += "▸ нет данных\n"

    report += f"\n📊 АКТИВНОСТЬ (последние {DAYS_BACK} дней):\n"
    report += f"▸ Сообщений: {activity_metrics['message_count']}\n"
    report += f"▸ Реакций: {activity_metrics['reaction_count']}\n"
    report += f"▸ Медиа: {activity_metrics['media']}\n"
    report += f"▸ Ссылок: {activity_metrics['links']}\n"
    if activity_metrics['message_count'] > 0:
        report += f"▸ Средняя длина: {activity_metrics['avg_msg_length']:.1f} симв.\n"
        report += f"▸ Доля медиа: {activity_metrics['media_ratio']*100:.1f}%\n"
        report += f"▸ Доля ссылок: {activity_metrics['link_ratio']*100:.1f}%\n"

    report += f"\n📈 ЧАСТОТА СЛОВ (топ-5):\n"
    if top_words:
        for word, count in top_words[:5]:
            report += f"▸ {word}: {count}\n"
    else:
        report += "▸ нет данных\n"

    report += f"\n🏆 РЕЙТИНГ: {rating} из 100\n"

    elapsed = (datetime.now() - start_time).total_seconds()
    report += f"\n📅 Отчёт за {elapsed:.1f} сек\n"

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
        await query.edit_message_text("Введите @username:", parse_mode="Markdown")
        user_states[update.effective_user.id] = "awaiting_username"
    elif query.data == "search_id":
        await query.edit_message_text("Введите ID:", parse_mode="Markdown")
        user_states[update.effective_user.id] = "awaiting_id"
    elif query.data == "help":
        await query.edit_message_text(
            "📖 Помощь\n\nБот показывает информацию о пользователе Telegram.\n"
            "Обычно отвечает за 5-15 секунд.",
            parse_mode="Markdown"
        )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = user_states.get(user_id)
    if not state:
        await update.message.reply_text("Нажмите /start")
        return
    if state == "awaiting_username":
        msg = await update.message.reply_text("🔍 Поиск...")
        result = await search_user_fast(text)
        await msg.edit_text(result)
    elif state == "awaiting_id":
        if not text.isdigit():
            await update.message.reply_text("❌ ID только цифры")
            return
        msg = await update.message.reply_text("🔍 Поиск...")
        result = await search_user_fast(int(text))
        await msg.edit_text(result)
    user_states.pop(user_id, None)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_states.pop(update.effective_user.id, None)
    await update.message.reply_text("Отменено. /start")

# ========== ЗАПУСК ==========
async def main():
    Thread(target=run_flask, daemon=True).start()
    logger.info("Веб-сервер запущен")

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
    except:
        await app.updater.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
