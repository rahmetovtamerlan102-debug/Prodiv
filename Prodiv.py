#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sqlite3
import logging
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ========== КОНФИГУРАЦИЯ ==========
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

if not API_ID or not API_HASH or not SESSION_STRING:
    print("❌ Ошибка: задай API_ID, API_HASH, SESSION_STRING")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== БАЗА ДАННЫХ ==========
DB_PATH = "private_cache.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS private_messages (
        chat_id INTEGER,
        message_id INTEGER,
        text TEXT,
        sender_id INTEGER,
        sender_name TEXT,
        date TEXT,
        PRIMARY KEY (chat_id, message_id)
    )''')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def save_private_message(chat_id, message_id, text, sender_id, sender_name, date):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO private_messages (chat_id, message_id, text, sender_id, sender_name, date)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (chat_id, message_id, text[:1000], sender_id, sender_name, date))
    conn.commit()
    conn.close()

def get_private_message(chat_id, message_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT text, sender_id, sender_name, date FROM private_messages WHERE chat_id = ? AND message_id = ?',
              (chat_id, message_id))
    row = c.fetchone()
    conn.close()
    return row

def delete_private_message(chat_id, message_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM private_messages WHERE chat_id = ? AND message_id = ?', (chat_id, message_id))
    conn.commit()
    conn.close()

# ========== КЛИЕНТ ==========
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ========== СОХРАНЕНИЕ ЛИЧНЫХ СООБЩЕНИЙ ==========
@client.on(events.NewMessage)
async def save_private(event):
    if event.is_private:
        msg = event.message
        if msg.text:
            sender_id = msg.sender_id
            sender_name = msg.sender.first_name if msg.sender else str(sender_id)
            save_private_message(
                chat_id=event.chat_id,
                message_id=msg.id,
                text=msg.text,
                sender_id=sender_id,
                sender_name=sender_name,
                date=msg.date.isoformat()
            )
            logger.debug(f"Сохранено ЛС сообщение {msg.id} от {sender_name}")

# ========== ОТСЛЕЖИВАНИЕ УДАЛЕНИЙ (ТОЛЬКО СОБЕСЕДНИК) ==========
@client.on(events.MessageDeleted)
async def on_private_deleted(event):
    if not event.is_private:
        return
    
    chat_id = event.chat_id
    deleted_ids = event.deleted_ids
    my_id = (await client.get_me()).id
    
    # Получаем имя собеседника
    try:
        entity = await client.get_entity(chat_id)
        chat_name = entity.first_name or str(chat_id)
    except:
        chat_name = f"Пользователь {chat_id}"
    
    for msg_id in deleted_ids:
        cached = get_private_message(chat_id, msg_id)
        
        if cached:
            text, sender_id, sender_name, msg_date = cached
            delete_private_message(chat_id, msg_id)
            
            # Если удалил НЕ ты (то есть удалил собеседник)
            if sender_id != my_id:
                report = f"🗑 **СОБЕСЕДНИК УДАЛИЛ СООБЩЕНИЕ**\n\n"
                report += f"👤 Собеседник: {chat_name}\n"
                report += f"📝 Текст: {text[:500]}\n"
                report += f"🕒 Отправлено: {msg_date}\n"
                report += f"🕒 Удалено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                
                # Отправляем уведомление в тот же чат
                await client.send_message(chat_id, report)
                logger.info(f"СОБЕСЕДНИК {chat_name} удалил сообщение")
            # Если удалил ты — ничего не отправляем
        else:
            # Текст не сохранён, но удаление мог произойти
            await client.send_message(chat_id, f"🗑 Собеседник удалил сообщение (текст не сохранён)")

# ========== ЗАПУСК ==========
async def main():
    init_db()
    await client.start()
    logger.info("✅ Бот запущен. Сохраняю личные сообщения, отслеживаю удаления собеседника...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
