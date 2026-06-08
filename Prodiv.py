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

# Подавляем предупреждения о циклах событий
import nest_asyncio
nest_asyncio.apply()

from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetCommonChatsRequest
from telethon.tl.types import InputUser, UserStatusOnline, UserStatusOffline

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

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
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

init_db()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def extract_links(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r'https?://[^\s]+', text))

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

# ========== КЛИЕНТ ==========
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

# ========== СБОР АКТИВНОСТИ ==========
async def collect_activity(client, target_user, common_chats, days_back=30):
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
        dc = dc_from_avatar(avatar) if avatar else None
        return user, bio, avatar, dc
    except Exception as e:
        logger.error(f"Ошибка full info: {e}")
        return target, None, None, None

async def get_common_chats(client, target_user):
    try:
        common = await client(GetCommonChatsRequest(
            user_id=InputUser(target_user.id, target_user.access_hash),
            max_id=0,
            limit=100
        ))
        return [{'id': c.id, 'title': getattr(c, 'title', str(c.id)), 'username': getattr(c, 'username', None)} for c in common.chats]
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

# ========== ПОИСК И ФОРМИРОВАНИЕ ОТЧЁТА ==========
async def search_user(target_input):
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

    user, bio, avatar, dc = await get_full_user_info(client, target)
    usernames = await get_all_usernames(client, user)
    common_chats = await get_common_chats(client, user)
    activity_metrics, processed_chats, word_counter = await collect_activity(client, user, common_chats, DAYS_BACK)
    rating = calculate_rating(activity_metrics)
    status_str = parse_status(getattr(user, 'status', None))

    await check_username_change(user.id, user.username or "нет")
    username_history = get_username_history(user.id)

    top_words = word_counter.most_common(10)
    reg_est = estimate_age_by_id(user.id)

    report = f"✈️ TELEGRAM · @{user.username or 'нет'}\n"
    report += "═" * 50 + "\n\n"
    report += f"🆔 ID: {user.id}\n"
    report += f"├─ активные username ({len(usernames)}): " + ", ".join([f"@{u}" for u in usernames[:5]]) + "\n"
    report += f"├─ имя: {user.first_name or ''} {user.last_name or ''}\n"
    if bio:
        report += f"├─ bio: {bio[:100]}\n"
    report += f"├─ DC: {dc if dc else 'не определён'}\n"
    report += f"├─ Premium: {'ДА' if getattr(user, 'premium', False) else 'НЕТ'}\n"
    report += f"├─ верифицирован: {'ДА' if getattr(user, 'verified', False) else 'НЕТ'}\n"
    report += f"├─ статус: {status_str}\n"
    report += f"├─ регистрация (оценочно): {reg_est}\n\n"

    report += f"🌀 История смены username:\n"
    if username_history:
        for old, new, date in username_history[:5]:
            date_str = date[:10] if date else "????-??-??"
            if old:
                report += f"├─ {date_str} → @{new} (был @{old})\n"
            else:
                report += f"├─ {date_str} → @{new}\n"
    else:
        report += f"└─ нет данных\n\n"

    report += f"📊 Активность (последние {DAYS_BACK} дней):\n"
    report += f"├─ сообщений: {activity_metrics['message_count']}\n"
    report += f"├─ реакций: {activity_metrics['reaction_count']}\n"
    report += f"├─ медиа: {activity_metrics['media']}\n"
    report += f"├─ ссылок: {activity_metrics['links']}\n"
    report += f"├─ средняя длина: {activity_metrics['avg_msg_length']:.1f} симв.\n"
    report += f"├─ доля медиа: {activity_metrics['media_ratio']*100:.1f}%\n"
    report += f"├─ доля ссылок: {activity_metrics['link_ratio']*100:.1f}%\n\n"

    report += f"📈 Частота слов (топ-5):\n"
    if top_words:
        for word, count in top_words[:5]:
            report += f"├─ {word}: {count}\n"
    else:
        report += f"└─ нет данных\n\n"

    report += f"🏆 Рейтинг: {rating} из 100\n"
    report += f"👥 Общих групп: {len(common_chats)}\n"
    report += "═" * 50

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
        "🤖 **Telegram User Info Bot**\n\n"
        "Выберите тип поиска:",
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
            "📖 **Помощь**\n\n"
            "Бот показывает информацию о пользователе Telegram:\n"
            "- ID, username, имя, био\n"
            "- Дата-центр, Premium, верификация\n"
            "- История смены username (если бот следил)\n"
            "- Активность в общих группах\n"
            "- Рейтинг активности (0-100)\n\n"
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
        await update.message.reply_text("🔍 Ищу информацию...")
        result = await search_user(text)
        await update.message.reply_text(result)
    elif state == "awaiting_id":
        if not text.isdigit():
            await update.message.reply_text("❌ ID должен состоять только из цифр.")
            return
        await update.message.reply_text("🔍 Ищу информацию...")
        result = await search_user(int(text))
        await update.message.reply_text(result)
    user_states.pop(user_id, None)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_states.pop(update.effective_user.id, None)
    await update.message.reply_text("Действие отменено. Нажмите /start")

# ========== ЗАПУСК ==========
async def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    logger.info("Бот запущен")
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
