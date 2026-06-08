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
from telethon.tl.types import InputUser, UserStatusOnline, UserStatusOffline

# Опциональная библиотека для подарков
try:
    from telegram_gift_fetcher import get_user_gifts
    HAS_GIFTS = True
    print("✅ Библиотека подарков загружена")
except ImportError as e:
    HAS_GIFTS = False
    print(f"⚠️ Библиотека подарков не установлена: {e}")
    print("   Подарки отображаться не будут")

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

# ========== СБОР АКТИВНОСТИ ==========
async def collect_activity(client, target_user, common_chats, days_back=30):
    metrics = defaultdict(int)
    word_counter = Counter()
    reaction_details = Counter()
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
                        if hasattr(r, 'reaction') and hasattr(r.reaction, 'emoticon'):
                            reaction_details[r.reaction.emoticon] += r.count
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

    return metrics, processed_chats, word_counter, reaction_details

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
        # Официальный рейтинг (баланс звёзд)
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

async def get_emoji_status(client, target_user):
    if hasattr(target_user, 'emoji_status') and target_user.emoji_status:
        return target_user.emoji_status
    return None

# ========== ПОДАРКИ (ЧЕРЕЗ БИБЛИОТЕКУ) ==========
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
                    sent.append({
                        'to_id': g.get('to_id'),
                        'type': g.get('type'),
                        'date': g.get('date')
                    })
            if 'received_gifts' in gifts:
                for g in gifts['received_gifts']:
                    received.append({
                        'from_id': g.get('from_id'),
                        'type': g.get('type'),
                        'date': g.get('date')
                    })
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

    user, bio, avatar, dc, full_user, official_rating = await get_full_user_info(client, target)
    usernames = await get_all_usernames(client, user)
    common_chats = await get_common_chats(client, user)
    activity_metrics, processed_chats, word_counter, reaction_details = await collect_activity(client, user, common_chats, DAYS_BACK)
    custom_rating = calculate_custom_rating(activity_metrics)
    status_str = parse_status(getattr(user, 'status', None))

    await check_username_change(user.id, user.username or "нет")
    username_history = get_username_history(user.id)

    top_words = word_counter.most_common(15)
    reg_est = estimate_age_by_id(user.id)
    gender = estimate_gender_by_name(user.first_name)

    # Подарки (опционально)
    sent_gifts, received_gifts = await get_gifts_info(client, user.id)

    # Эмодзи-статус
    emoji_status = await get_emoji_status(client, user)

    # Формирование отчёта
    report = f"✈️ Telegram · @{user.username or 'нет'}\n"
    report += f"▸ ID: {user.id}\n"
    if usernames:
        report += f"▸ Активные username ({len(usernames)}): " + ", ".join([f"@{u}" for u in usernames]) + "\n"
    report += f"▸ Имя: {user.first_name or ''} {user.last_name or ''}\n"
    report += f"▸ Пол: {gender}\n"
    if bio:
        report += f"▸ Bio: {bio[:200]}\n"
    report += f"▸ Дата-центр (DC): {dc if dc else 'не определён'}\n"
    report += f"▸ Telegram Premium: {'Да' if getattr(user, 'premium', False) else 'Нет'}\n"
    if getattr(user, 'premium', False) and hasattr(full_user, 'premium_expires'):
        expires = full_user.premium_expires
        if expires:
            report += f"▸ Premium до: {expires.strftime('%Y-%m-%d')}\n"
    report += f"▸ Верифицирован: {'Да' if getattr(user, 'verified', False) else 'Нет'}\n"
    report += f"▸ Является ботом: {'Да' if user.bot else 'Нет'}\n"
    report += f"▸ Статус: {status_str}\n"
    report += f"▸ Регистрация (оценочно): {reg_est}\n"
    if emoji_status:
        report += f"▸ Эмодзи-статус: {emoji_status}\n"
    if official_rating is not None:
        report += f"▸ Официальный рейтинг Telegram: {official_rating} ⭐️\n"

    report += f"\n🌀 История изменения username:\n"
    if username_history:
        for old, new, date in username_history[:10]:
            date_str = date[:10] if date else "????-??-??"
            if old:
                report += f"▸ {date_str} → @{new} (был @{old})\n"
            else:
                report += f"▸ {date_str} → @{new}\n"
    else:
        report += f"▸ нет данных\n"

    # Подарки (отправленные)
    if sent_gifts and len(sent_gifts) > 0:
        report += f"\n🎁 Подарки (отправленные):\n"
        report += f"▸ Всего отправлено: {len(sent_gifts)}\n"
        gift_ids = [f"#{g['to_id']}" for g in sent_gifts[:30] if g.get('to_id')]
        if gift_ids:
            report += f"▸ Кому отправлял: " + " · ".join(gift_ids) + "\n"
    else:
        report += f"\n🎁 Подарки (отправленные): нет данных\n"

    # Подарки (полученные)
    if received_gifts and len(received_gifts) > 0:
        report += f"🎁 Подарки (полученные):\n"
        report += f"▸ Всего получено: {len(received_gifts)}\n"
        gift_from = [f"#{g['from_id']}" for g in received_gifts[:30] if g.get('from_id')]
        if gift_from:
            report += f"▸ От кого: " + " · ".join(gift_from) + "\n"

    report += f"\n📊 Активность (последние {DAYS_BACK} дней):\n"
    report += f"▸ Сообщений: {activity_metrics['message_count']}\n"
    report += f"▸ Реакций получено: {activity_metrics['reaction_count']}\n"
    if reaction_details:
        top_reactions = reaction_details.most_common(5)
        if top_reactions:
            report += f"▸ Реакции: " + ", ".join([f"{r}: {c}" for r, c in top_reactions]) + "\n"
    report += f"▸ Медиа: {activity_metrics['media']}\n"
    report += f"▸ Ссылок: {activity_metrics['links']}\n"
    report += f"▸ Средняя длина сообщения: {activity_metrics['avg_msg_length']:.1f} симв.\n"
    report += f"▸ Доля медиа: {activity_metrics['media_ratio']*100:.1f}%\n"
    report += f"▸ Доля ссылок: {activity_metrics['link_ratio']*100:.1f}%\n"

    report += f"\n📈 Частота слов (топ-10):\n"
    if top_words:
        for word, count in top_words[:10]:
            report += f"▸ {word}: {count}\n"
    else:
        report += f"▸ нет данных\n"

    report += f"\n🏆 Кастомный рейтинг активности: {custom_rating} из 100\n"
    report += f"👥 Общих групп: {len(common_chats)}\n"

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
            "- Дата-центр (DC)\n"
            "- Premium, верификация, эмодзи-статус\n"
            "- История смены username\n"
            "- Подарки (отправленные и полученные)\n"
            "- Активность в общих группах (сообщения, реакции, медиа, ссылки)\n"
            "- Частота слов\n"
            "- Кастомный рейтинг активности\n"
            "- Официальный рейтинг Telegram (если доступен)\n\n"
            "Поиск может занять до минуты.\n\n"
            "История username накапливается с момента первого поиска.",
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
        msg = await update.message.reply_text("🔍 Ищу информацию... Это может занять некоторое время.")
        result = await search_user(text)
        await msg.edit_text(result)
    elif state == "awaiting_id":
        if not text.isdigit():
            await update.message.reply_text("❌ ID должен состоять только из цифр.")
            return
        msg = await update.message.reply_text("🔍 Ищу информацию... Это может занять некоторое время.")
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
