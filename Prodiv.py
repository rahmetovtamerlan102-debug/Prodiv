#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sqlite3
import logging
import requests
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ========== КОНФИГУРАЦИЯ ==========
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = 8276815852  # ТВОЙ ЛИЧНЫЙ ID

if not API_ID or not API_HASH or not SESSION_STRING or not BOT_TOKEN:
    print("❌ Ошибка: задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = "private_cache.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS private_messages (
            chat_id INTEGER,
            message_id INTEGER,
            text TEXT,
            sender_id INTEGER,
            sender_name TEXT,
            date TEXT,
            PRIMARY KEY (chat_id, message_id)
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def save_message(chat_id, msg_id, text, sender_id, sender_name, date):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO private_messages
        (chat_id, message_id, text, sender_id, sender_name, date)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (chat_id, msg_id, text[:1000], sender_id, sender_name, date))
    conn.commit()
    conn.close()

def get_message(chat_id, msg_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT text, sender_id, sender_name, date FROM private_messages WHERE chat_id = ? AND message_id = ?',
              (chat_id, msg_id))
    row = c.fetchone()
    conn.close()
    return row

def delete_message(chat_id, msg_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM private_messages WHERE chat_id = ? AND message_id = ?', (chat_id, msg_id))
    conn.commit()
    conn.close()

def send_via_bot(text: str):
    """Бот пишет тебе в личку"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": ADMIN_ID,
        "text": text,
        "parse_mode": "markdown"
    }
    try:
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"Уведомление отправлено админу {ADMIN_ID}")
        else:
            logger.error(f"Ошибка API: {r.text}")
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@client.on(events.NewMessage)
async def save_private(event):
    if event.is_private:
        msg = event.message
        if msg.text:
            sender_id = msg.sender_id
            sender_name = msg.sender.first_name if msg.sender else str(sender_id)
            save_message(
                chat_id=event.chat_id,
                msg_id=msg.id,
                text=msg.text,
                sender_id=sender_id,
                sender_name=sender_name,
                date=msg.date.isoformat()
            )
            logger.debug(f"Сохранено ЛС сообщение {msg.id} от {sender_name}")

@client.on(events.MessageDeleted)
async def on_private_deleted(event):
    if not event.is_private:
        return

    chat_id = event.chat_id
    deleted_ids = event.deleted_ids

    try:
        entity = await client.get_entity(chat_id)
        chat_name = entity.first_name or str(chat_id)
    except:
        chat_name = f"Пользователь {chat_id}"

    my_id = (await client.get_me()).id

    for msg_id in deleted_ids:
        cached = get_message(chat_id, msg_id)
        if cached:
            text, sender_id, sender_name, msg_date = cached
            delete_message(chat_id, msg_id)

            if sender_id != my_id:
                report = (
                    f"🗑 **СОБЕСЕДНИК УДАЛИЛ СООБЩЕНИЕ**\n\n"
                    f"👤 Собеседник: {chat_name}\n"
                    f"📝 Текст: {text[:500]}\n"
                    f"🕒 Отправлено: {msg_date}\n"
                    f"🕒 Удалено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                send_via_bot(report)
        else:
            send_via_bot(f"🗑 Собеседник {chat_name} удалил сообщение (текст не сохранён)")

async def main():
    init_db()
    await client.start()
    me = await client.get_me()
    logger.info(f"✅ Telethon запущен от имени @{me.username}")
    
    send_via_bot("🤖 Система мониторинга удалений запущена")
    
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
