#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import logging
import logging.handlers
import signal
import time
import sqlite3
import statistics
import hashlib
from collections import defaultdict
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    UserStatusOnline, UserStatusOffline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth, UserStatusEmpty
)

# ========== КОНФИГ ==========
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TIMEOUT = 15  # уменьшил с 30 до 15 секунд для скорости
DB_FILE = "monitor.db"
MONITOR_INTERVAL = 300

if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
    raise Exception("Задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")

# ========== ЛОГИ ==========
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.handlers.RotatingFileHandler('monitor.log', maxBytes=1_000_000, backupCount=5)
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

# ========== ГЛОБАЛЬНЫЕ КЛИЕНТЫ ==========
user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot_client = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== ИНИЦИАЛИЗАЦИЯ БД ==========
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, success INTEGER, rt INTEGER,
        bot_id INTEGER, bot_name TEXT, description TEXT,
        premium INTEGER, verified INTEGER, scam INTEGER, fake INTEGER,
        status_text TEXT, dc_id INTEGER, account_age TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS raw_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, success INTEGER, rt INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS deep_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER,
        cold_ms INTEGER, warm_ms INTEGER,
        stability_ok INTEGER, stability_total INTEGER,
        unknown_rate REAL, empty_rate REAL,
        load_5_rate INTEGER, load_timeout INTEGER,
        jitter_ms INTEGER, trend TEXT, status TEXT, score INTEGER,
        ttfb_ms INTEGER, processing_ms INTEGER, burst_avg_ms INTEGER, burst_loss INTEGER,
        session_decay_start INTEGER, session_decay_end INTEGER,
        ux_index INTEGER, robustness TEXT, fingerprint TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS response_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, size INTEGER, resp_type TEXT,
        has_buttons INTEGER, has_media INTEGER, duplicate_hash TEXT,
        complexity TEXT, button_count INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS context_memory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, remembers INTEGER, forgets INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS recovery_behavior (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, auto_retry INTEGER, restart_flow INTEGER, crash INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS unknown_cmd_class (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, help_response INTEGER, ignore INTEGER, error_response INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS loop_detection (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, loop_detected INTEGER, loop_pattern TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS hourly_perf (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, hour INTEGER, avg_rt INTEGER, sample_count INTEGER,
        UNIQUE(bot_username, hour)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS health_map (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, ts INTEGER, overall_health TEXT
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_raw ON raw_metrics(bot_username, ts)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_deep ON deep_stats(bot_username, ts)')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

# ========== ФУНКЦИИ ПЕРЕПОДКЛЮЧЕНИЯ ==========
async def ensure_connection():
    """Проверяет и восстанавливает соединение Telethon"""
    try:
        if not user_client.is_connected():
            logger.warning("Клиент отключён, переподключаюсь...")
            await user_client.connect()
        
        if not await user_client.is_user_authorized():
            logger.warning("Сессия не авторизована, перезапускаю...")
            await user_client.start()
        
        # Проверяем, что соединение реально работает
        await user_client.get_me()
        logger.debug("Соединение стабильно")
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения: {e}")
        # Пробуем переподключиться принудительно
        try:
            await user_client.disconnect()
            await asyncio.sleep(1)
            await user_client.connect()
            await user_client.start()
            await user_client.get_me()
            logger.info("Принудительное переподключение успешно")
            return True
        except Exception as e2:
            logger.error(f"Не удалось переподключиться: {e2}")
            return False

async def safe_send_and_wait(entity, text: str, timeout=TIMEOUT):
    """Отправка запроса с автоматическим переподключением при ошибке"""
    try:
        return await send_and_wait(entity, text, timeout)
    except (ConnectionError, OSError, AttributeError, Exception) as e:
        logger.error(f"Ошибка при отправке: {e}. Переподключаюсь...")
        if await ensure_connection():
            try:
                return await send_and_wait(entity, text, timeout)
            except Exception as e2:
                logger.error(f"Повторная отправка также не удалась: {e2}")
                return False, None, None
        return False, None, None

# ========== БАЗОВЫЕ ФУНКЦИИ СОХРАНЕНИЯ ==========
def save_check(bot: str, success: bool, rt: int, info: dict):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO checks (bot_username, ts, success, rt, bot_id, bot_name,
        description, premium, verified, scam, fake, status_text, dc_id, account_age)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (bot, int(time.time()), 1 if success else 0, rt, info.get('id'),
         info.get('name'), info.get('description'), 1 if info.get('premium') else 0,
         1 if info.get('verified') else 0, 1 if info.get('scam') else 0, 1 if info.get('fake') else 0,
         info.get('status_text'), info.get('dc_id'), info.get('account_age')))
    conn.commit()
    conn.close()

def save_raw(bot: str, success: bool, rt: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO raw_metrics (bot_username, ts, success, rt) VALUES (?,?,?,?)',
              (bot, int(time.time()), 1 if success else 0, rt if success else None))
    conn.commit()
    conn.close()

def save_deep(bot: str, data: dict):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO deep_stats (bot_username, ts, cold_ms, warm_ms,
        stability_ok, stability_total, unknown_rate, empty_rate,
        load_5_rate, load_timeout, jitter_ms, trend, status, score,
        ttfb_ms, processing_ms, burst_avg_ms, burst_loss,
        session_decay_start, session_decay_end, ux_index, robustness, fingerprint)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (bot, int(time.time()), data.get('cold_ms'), data.get('warm_ms'),
         data.get('stability_ok'), data.get('stability_total'),
         data.get('unknown_rate'), data.get('empty_rate'),
         data.get('load_5_rate'), data.get('load_timeout'),
         data.get('jitter_ms'), data.get('trend'), data.get('status'), data.get('score'),
         data.get('ttfb_ms'), data.get('processing_ms'), data.get('burst_avg_ms'), data.get('burst_loss'),
         data.get('session_decay_start'), data.get('session_decay_end'),
         data.get('ux_index'), data.get('robustness'), data.get('fingerprint')))
    conn.commit()
    conn.close()

def save_response_detail(bot: str, size: int, resp_type: str, has_buttons: bool, has_media: bool, dup_hash: str, complexity: str, button_count: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO response_details (bot_username, ts, size, resp_type, has_buttons, has_media, duplicate_hash, complexity, button_count)
                 VALUES (?,?,?,?,?,?,?,?,?)''',
              (bot, int(time.time()), size, resp_type, 1 if has_buttons else 0, 1 if has_media else 0, dup_hash, complexity, button_count))
    conn.commit()
    conn.close()

def save_context(bot: str, remembers: int, forgets: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO context_memory (bot_username, ts, remembers, forgets) VALUES (?,?,?,?)',
              (bot, int(time.time()), remembers, forgets))
    conn.commit()
    conn.close()

def save_recovery(bot: str, auto_retry: int, restart: int, crash: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO recovery_behavior (bot_username, ts, auto_retry, restart_flow, crash) VALUES (?,?,?,?,?)',
              (bot, int(time.time()), auto_retry, restart, crash))
    conn.commit()
    conn.close()

def save_unknown_class(bot: str, help_resp: int, ignore: int, err_resp: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO unknown_cmd_class (bot_username, ts, help_response, ignore, error_response) VALUES (?,?,?,?,?)',
              (bot, int(time.time()), help_resp, ignore, err_resp))
    conn.commit()
    conn.close()

def save_loop(bot: str, detected: bool, pattern: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO loop_detection (bot_username, ts, loop_detected, loop_pattern) VALUES (?,?,?,?)',
              (bot, int(time.time()), 1 if detected else 0, pattern))
    conn.commit()
    conn.close()

def update_hourly(bot: str, hour: int, rt: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO hourly_perf (bot_username, hour, avg_rt, sample_count)
                 VALUES (?, ?, ?, 1) ON CONFLICT(bot_username, hour) DO UPDATE SET
                 avg_rt = (avg_rt * sample_count + excluded.avg_rt) / (sample_count + 1),
                 sample_count = sample_count + 1''', (bot, hour, rt))
    conn.commit()
    conn.close()

def save_health(bot: str, health: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO health_map (bot_username, ts, overall_health) VALUES (?,?,?)',
              (bot, int(time.time()), health))
    conn.commit()
    conn.close()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def get_status_text(st) -> str:
    if isinstance(st, UserStatusOnline): return "онлайн"
    if isinstance(st, UserStatusOffline): return "офлайн"
    if isinstance(st, UserStatusRecently): return "был недавно"
    return "неизвестно"

def estimate_age(uid: int) -> str:
    if uid < 1_000_000: return "2013-2014"
    if uid < 10_000_000: return "2015-2016"
    if uid < 50_000_000: return "2017-2018"
    if uid < 100_000_000: return "2019-2020"
    return "2021+"

async def send_and_wait(entity, text: str, timeout=TIMEOUT):
    """Базовая функция отправки и ожидания ответа"""
    start = time.time()
    future = asyncio.get_event_loop().create_future()
    resp = None
    
    def handler(e):
        nonlocal resp
        if not future.done():
            resp = e.message
            future.set_result(True)
    
    user_client.add_event_handler(handler, events.NewMessage(from_users=entity.id))
    try:
        await user_client.send_message(entity, text)
        await asyncio.wait_for(future, timeout=timeout)
        elapsed = round((time.time() - start) * 1000)
        return True, elapsed, resp
    except asyncio.TimeoutError:
        return False, None, None
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass

async def get_bot_info(username: str) -> Tuple[Optional[Any], Dict]:
    await ensure_connection()
    try:
        entity = await user_client.get_entity(username)
        info = {
            'id': entity.id,
            'name': entity.first_name or entity.title,
            'verified': getattr(entity, 'verified', False),
            'scam': getattr(entity, 'scam', False),
            'fake': getattr(entity, 'fake', False),
            'premium': getattr(entity, 'premium', False),
            'status_text': get_status_text(entity.status) if hasattr(entity,'status') else 'неизвестно',
            'dc_id': entity.photo.dc_id if hasattr(entity,'photo') and entity.photo else None,
            'account_age': estimate_age(entity.id)
        }
        try:
            full = await user_client(GetFullUserRequest(username))
            if full and full.bot_info:
                info['description'] = full.bot_info.description
        except:
            info['description'] = ''
        return entity, info
    except Exception as e:
        return None, {'error': str(e)}

async def quick_check(bot_username: str) -> Tuple[bool, int, Dict]:
    await ensure_connection()
    entity, info = await get_bot_info(bot_username)
    if entity is None:
        return False, 0, info
    ok, rt, _ = await safe_send_and_wait(entity, "/start")
    success = ok and rt is not None
    if success:
        save_raw(bot_username, True, rt)
        save_check(bot_username, True, rt, info)
        update_hourly(bot_username, datetime.now().hour, rt)
        return True, rt, info
    else:
        save_raw(bot_username, False, None)
        save_check(bot_username, False, None, info)
        return False, 0, info

# ========== ВСЕ МЕТРИКИ (ГЛУБОКИЙ АНАЛИЗ) ==========
async def full_deep_check(bot_username: str) -> Dict:
    await ensure_connection()
    entity, info = await get_bot_info(bot_username)
    if entity is None:
        return {'error': info.get('error', 'no entity')}
    result = {}

    # ---- Холодный/тёплый старт, TTFB ----
    ok1, t1, msg1 = await safe_send_and_wait(entity, "/start")
    if not ok1:
        return {'error': 'бот не отвечает на /start (таймаут)'}
    result['cold_ms'] = t1
    result['ttfb_ms'] = t1
    result['processing_ms'] = 0
    await asyncio.sleep(1)
    ok2, t2, _ = await safe_send_and_wait(entity, "/start")
    result['warm_ms'] = t2 if ok2 else None

    # ---- Стабильность 10 раз ----
    stable_ok = 0
    times = []
    for _ in range(10):
        ok, rt, _ = await safe_send_and_wait(entity, "/start")
        if ok:
            stable_ok += 1
            times.append(rt)
        await asyncio.sleep(0.3)
    result['stability_ok'] = stable_ok
    result['stability_total'] = 10
    result['jitter_ms'] = round(statistics.stdev(times)) if len(times) >= 3 else 0

    # ---- Неизвестные команды и пустые ответы ----
    unknown = 0
    empty = 0
    for cmd in ['/xyz123', '!@#$', 'random_garbage']:
        ok, _, msg = await safe_send_and_wait(entity, cmd, timeout=8)
        if not ok:
            unknown += 1
        elif msg and len(msg.raw_text or '') < 5:
            empty += 1
        await asyncio.sleep(0.3)
    result['unknown_rate'] = round(unknown/3*100, 1)
    result['empty_rate'] = round(empty/3*100, 1)

    # ---- Нагрузочный тест 5 запросов/сек ----
    load_ok = 0
    for _ in range(5):
        ok, rt, _ = await safe_send_and_wait(entity, "/start", timeout=5)
        if ok:
            load_ok += 1
        await asyncio.sleep(0.2)
    result['load_5_rate'] = load_ok
    result['load_timeout'] = 5 - load_ok

    # ---- Пиковая нагрузка burst 5 запросов (уменьшил с 10) ----
    burst_times = []
    burst_loss = 0
    tasks = []
    async def one_req():
        ok, rt, _ = await safe_send_and_wait(entity, "/start", timeout=5)
        return ok, rt
    for _ in range(5):
        tasks.append(one_req())
        await asyncio.sleep(0.05)
    results = await asyncio.gather(*tasks)
    for ok, rt in results:
        if ok:
            burst_times.append(rt)
        else:
            burst_loss += 1
    result['burst_avg_ms'] = round(statistics.mean(burst_times)) if burst_times else 0
    result['burst_loss'] = burst_loss

    # ---- Сессионная деградация (8 шагов вместо 15) ----
    session_times = []
    for i in range(8):
        ok, rt, _ = await safe_send_and_wait(entity, "/start")
        if ok:
            session_times.append(rt)
        await asyncio.sleep(0.5)
    if len(session_times) >= 6:
        result['session_decay_start'] = round(statistics.mean(session_times[:3]))
        result['session_decay_end'] = round(statistics.mean(session_times[-3:]))
    else:
        result['session_decay_start'] = None
        result['session_decay_end'] = None

    # ---- Тренд, статус, скор ----
    trend = get_trend(bot_username)
    result['trend'] = f"{trend['direction']} ({trend['change']:+}%)"
    status = get_status(bot_username)
    result['status'] = status
    speed_score = max(0, min(40, 40 - (t1 - 100)/25))
    stability_score = (stable_ok / 10) * 30
    logic_score = max(0, 30 - result['unknown_rate']*0.3 - result['empty_rate']*0.2)
    result['score'] = int(speed_score + stability_score + logic_score)

    # ---- Контекстная память ----
    await safe_send_and_wait(entity, "Меня зовут Тест")
    await asyncio.sleep(1)
    ok, _, msg = await safe_send_and_wait(entity, "Как меня зовут?")
    remembers = 1 if ok and msg and "тест" in (msg.raw_text or '').lower() else 0
    forgets = 1 - remembers
    save_context(bot_username, remembers, forgets)

    # ---- Восстановление после ошибки ----
    ok1, _, _ = await safe_send_and_wait(entity, "x"*3000, timeout=5)  # уменьшил длину
    ok2, _, _ = await safe_send_and_wait(entity, "/start", timeout=8)
    auto_retry = 1 if not ok1 and ok2 else 0
    crash = 1 if not ok1 and not ok2 else 0
    restart = 1 if ok1 and ok2 else 0
    save_recovery(bot_username, auto_retry, restart, crash)

    # ---- Классификация неизвестных команд ----
    help_resp = ignore = err_resp = 0
    for cmd in ['random_xyz', 'unknown_command']:
        ok, _, msg = await safe_send_and_wait(entity, cmd, timeout=5)
        if not ok:
            ignore += 1
        elif msg and ("помощь" in (msg.raw_text or '').lower() or "help" in (msg.raw_text or '').lower()):
            help_resp += 1
        elif msg and ("ошибка" in (msg.raw_text or '').lower()):
            err_resp += 1
        else:
            ignore += 1
    save_unknown_class(bot_username, help_resp, ignore, err_resp)

    # ---- Детекция циклов ----
    history = []
    for _ in range(4):  # уменьшил с 5
        ok, _, msg = await safe_send_and_wait(entity, "/start")
        if ok and msg:
            h = hashlib.md5((msg.raw_text or '').encode()).hexdigest()[:8]
            history.append(h)
        await asyncio.sleep(0.5)
    loop_detected = len(set(history)) < len(history) and len(history) > 2
    save_loop(bot_username, loop_detected, str(history))

    # ---- Стиль и сложность ответа ----
    ok, _, msg = await safe_send_and_wait(entity, "/start")
    if ok and msg:
        text = msg.raw_text or ''
        size = len(text.encode('utf-8'))
        has_buttons = bool(msg.buttons)
        has_media = bool(msg.media)
        button_count = len(msg.buttons[0]) if msg.buttons and msg.buttons[0] else 0
        if has_buttons and has_media:
            rt = "mixed"
        elif has_buttons:
            rt = "buttons"
        elif has_media:
            rt = "media"
        else:
            rt = "text"
        dup_hash = hashlib.md5(text.encode()).hexdigest() if text else None
        if button_count > 5 or has_media:
            comp = "heavy"
        elif len(text) > 200 or button_count > 0:
            comp = "medium"
        else:
            comp = "simple"
        save_response_detail(bot_username, size, rt, has_buttons, has_media, dup_hash, comp, button_count)

    # ---- UX индекс и прочее ----
    ux = calculate_ux_index(bot_username)
    result['ux_index'] = ux
    robustness = "High" if result['score'] >= 70 else "Medium" if result['score'] >= 40 else "Low"
    result['robustness'] = robustness
    fingerprint = generate_fingerprint(bot_username)
    result['fingerprint'] = fingerprint
    overall = "🟢 Good" if result['score'] >= 70 else "🟡 Fair" if result['score'] >= 40 else "🔴 Poor"
    save_health(bot_username, overall)

    save_deep(bot_username, result)
    return result

# ========== ФУНКЦИИ ДЛЯ СТАТИСТИКИ ==========
def get_trend(bot: str) -> Dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT rt FROM raw_metrics WHERE bot_username=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT 30', (bot,))
    rows = c.fetchall()
    conn.close()
    if len(rows) < 6:
        return {'direction': 'unknown', 'change': 0}
    times = [r[0] for r in rows]
    mid = len(times)//2
    first = times[:mid]
    last = times[mid:]
    avg_first = statistics.mean(first)
    avg_last = statistics.mean(last)
    change = round((avg_last - avg_first) / avg_first * 100)
    direction = 'degrading' if change > 20 else 'improving' if change < -20 else 'stable'
    return {'direction': direction, 'change': change}

def get_status(bot: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT success FROM raw_metrics WHERE bot_username=? ORDER BY ts DESC LIMIT 20', (bot,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return 'unknown'
    success_rate = sum(1 for r in rows if r[0]) / len(rows) * 100
    if success_rate >= 90:
        return 'stable'
    elif success_rate >= 70:
        return 'warning'
    else:
        return 'critical'

def get_stats(bot: str, limit: int = 20) -> Dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT rt FROM raw_metrics WHERE bot_username=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT ?', (bot, limit))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return {'avg_time': None, 'count': 0}
    times = [r[0] for r in rows]
    return {'avg_time': statistics.mean(times), 'count': len(times)}

def get_advanced_stats(bot: str, hours: int = 24) -> Dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    since = int(time.time()) - hours*3600
    c.execute('SELECT success, rt FROM raw_metrics WHERE bot_username=? AND ts>=?', (bot, since))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return {'uptime': 0, 'avg_time': None, 'jitter_ms': None}
    total = len(rows)
    success = sum(1 for r in rows if r[0])
    uptime = round(success/total*100, 1)
    times = [r[1] for r in rows if r[1] is not None]
    avg = statistics.mean(times) if times else None
    jitter = round(statistics.stdev(times), 1) if len(times) >= 2 else None
    return {'uptime': uptime, 'avg_time': avg, 'jitter_ms': jitter}

def get_latest_deep(bot: str) -> Optional[Dict]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''SELECT cold_ms, warm_ms, stability_ok, stability_total, unknown_rate, empty_rate,
                      load_5_rate, jitter_ms, score, ttfb_ms, processing_ms, burst_avg_ms, burst_loss,
                      session_decay_start, session_decay_end, ux_index, robustness, fingerprint
                 FROM deep_stats WHERE bot_username=? ORDER BY ts DESC LIMIT 1''', (bot,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {'cold_ms': row[0], 'warm_ms': row[1], 'stability_ok': row[2], 'stability_total': row[3],
            'unknown_rate': row[4], 'empty_rate': row[5], 'load_5_rate': row[6], 'jitter_ms': row[7],
            'score': row[8], 'ttfb_ms': row[9], 'processing_ms': row[10], 'burst_avg_ms': row[11],
            'burst_loss': row[12], 'session_decay_start': row[13], 'session_decay_end': row[14],
            'ux_index': row[15], 'robustness': row[16], 'fingerprint': row[17]}

def calculate_ux_index(bot: str) -> int:
    stats = get_stats(bot, 20)
    adv = get_advanced_stats(bot, 24)
    deep = get_latest_deep(bot)
    if not deep:
        return 50
    speed_score = max(0, min(30, 30 - (stats.get('avg_time', 1000)-100)/30))
    stability_score = max(0, min(30, adv.get('uptime', 0) * 0.3))
    logic_score = max(0, min(30, 30 - deep.get('unknown_rate', 0)*0.3 - deep.get('empty_rate', 0)*0.2))
    interactive_score = 10
    return int(speed_score + stability_score + logic_score + interactive_score)

def generate_fingerprint(bot: str) -> str:
    deep = get_latest_deep(bot)
    if not deep:
        return "недостаточно данных"
    speed = "Fast" if deep.get('cold_ms', 500) < 300 else "Medium" if deep.get('cold_ms', 500) < 800 else "Slow"
    stability = "High" if deep.get('jitter_ms', 100) < 50 else "Medium" if deep.get('jitter_ms', 100) < 150 else "Low"
    errors = "Low" if deep.get('unknown_rate', 100) < 10 else "Medium" if deep.get('unknown_rate', 100) < 30 else "High"
    return f"{speed} responder, {stability} stability, {errors} errors"

# ========== KEEP-ALIVE ДЛЯ RENDER ==========
async def keep_alive_loop():
    """Фоновый процесс: каждые 30 секунд проверяет соединение и не даёт боту заснуть"""
    while True:
        await asyncio.sleep(30)
        try:
            # Поддерживаем соединение Telethon
            if user_client.is_connected():
                await user_client.get_me()
            else:
                await ensure_connection()
            
            # Поддерживаем бота (чтобы Render не усыплял)
            await bot_client.get_me()
            
        except Exception as e:
            logger.error(f"Keep-alive ошибка: {e}")
            await ensure_connection()

# ========== ФОРМИРОВАНИЕ ОТЧЁТА ==========
async def generate_full_report(bot_username: str) -> str:
    deep = await full_deep_check(bot_username)
    if 'error' in deep:
        return f"❌ Ошибка: {deep['error']}"
    
    _, info = await get_bot_info(bot_username)
    stats = get_stats(bot_username, 20)
    adv = get_advanced_stats(bot_username, 24)
    status = get_status(bot_username)
    trend = get_trend(bot_username)
    
    lines = []
    lines.append(f"🤖 **ФУЛЛ ОТЧЁТ** @{bot_username}")
    lines.append("════════════════════════════════════════════════════════════════════════════════")
    lines.append("")
    lines.append("📌 1. ОБЩАЯ ИНФОРМАЦИЯ О БОТЕ")
    lines.append("────────────────────────────────────────────────────────────────────────────")
    lines.append(f"   • ID: `{info.get('id')}`")
    lines.append(f"   • Имя: {info.get('name')}")
    desc = info.get('description', '')
    lines.append(f"   • Описание: {desc[:100]}{'...' if len(desc)>100 else ''}")
    lines.append(f"   • Верифицирован: {'✅ Да' if info.get('verified') else '❌ Нет'}")
    lines.append(f"   • Премиум: {'✅ Да' if info.get('premium') else '❌ Нет'}")
    lines.append(f"   • Статус аккаунта: {info.get('status_text')}")
    lines.append(f"   • Возраст аккаунта: {info.get('account_age')}")
    lines.append("")
    
    status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
    lines.append(f"🟢 2. СТАТУС И ПРОИЗВОДИТЕЛЬНОСТЬ")
    lines.append("────────────────────────────────────────────────────────────────────────────")
    lines.append(f"   • Итоговый статус: {status_emoji} {status.upper()}")
    lines.append(f"   • Средняя скорость: {stats.get('avg_time', 0):.0f} мс")
    lines.append(f"   • Джиттер (σ): {deep.get('jitter_ms', 0)} мс → {'LOW' if deep.get('jitter_ms', 99) < 50 else 'MEDIUM' if deep.get('jitter_ms', 99) < 150 else 'HIGH'}")
    lines.append(f"   • Тренд: {trend['direction']} ({trend['change']:+}%)")
    lines.append(f"   • Доступность (uptime): {adv.get('uptime', 0)}%")
    lines.append("")
    
    lines.append("🧪 3. ГЛУБОКИЙ АНАЛИЗ")
    lines.append("────────────────────────────────────────────────────────────────────────────")
    lines.append(f"   • Холодный старт: {deep.get('cold_ms', 0)} мс")
    lines.append(f"   • Тёплый ответ: {deep.get('warm_ms', 0)} мс")
    lines.append(f"   • Стабильность 10 запросов: {deep.get('stability_ok',0)}/10")
    lines.append(f"   • Нагрузка 5/сек: {deep.get('load_5_rate',0)}/5")
    lines.append(f"   • Burst 5 запросов: среднее {deep.get('burst_avg_ms',0)} мс, потерь {deep.get('burst_loss',0)}/5")
    lines.append("")
    
    lines.append("🏆 4. ИТОГОВАЯ ОЦЕНКА")
    lines.append("────────────────────────────────────────────────────────────────────────────")
    lines.append(f"   • **Бот скор: {deep.get('score',0)}/100**")
    lines.append(f"   • Устойчивость логики: {deep.get('robustness', '—')}")
    lines.append(f"   • UX индекс: {deep.get('ux_index', 0)}/100")
    lines.append(f"   • Отпечаток: {deep.get('fingerprint', '—')}")
    lines.append("")
    
    lines.append("════════════════════════════════════════════════════════════════════════════════")
    return "\n".join(lines)

# ========== ОБРАБОТЧИКИ КОМАНД ==========
@dp.message(Command('start'))
async def cmd_start(msg: Message):
    await msg.reply(
        "🤖 **Монитор ботов**\n\n"
        "Команды:\n"
        "/check @bot – быстрая проверка (2-3 сек)\n"
        "/fullreport @bot – полный отчёт (30-60 сек)\n"
        "/stats @bot – статистика\n"
        "/add @bot – добавить в авто-мониторинг\n"
        "/remove @bot – удалить\n"
        "/list – список отслеживаемых"
    )

@dp.message(Command('check'))
async def cmd_check(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /check @username")
    username = args[1].lstrip('@')
    status_msg = await msg.reply(f"🔎 Проверяю @{username}...")
    success, rt, info = await quick_check(username)
    status = get_status(username)
    status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
    report = f"🤖 @{username}\n{status_emoji} Статус: {status.upper()}\n⚡ Время: {rt} мс"
    await status_msg.edit_text(report)

@dp.message(Command('fullreport'))
async def cmd_fullreport(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /fullreport @username")
    username = args[1].lstrip('@')
    status_msg = await msg.reply(f"🔍 Анализ @{username} (30-60 сек)...")
    report = await generate_full_report(username)
    await status_msg.edit_text(report)

@dp.message(Command('stats'))
async def cmd_stats(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /stats @username")
    username = args[1].lstrip('@')
    stats = get_stats(username, 20)
    if stats['count'] == 0:
        return await msg.reply(f"Нет данных по @{username}")
    await msg.reply(f"📊 @{username}\nСреднее время: {stats['avg_time']:.0f} мс\nЗамеров: {stats['count']}")

@dp.message(Command('add'))
async def cmd_add(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /add @username")
    username = args[1].lstrip('@')
    watched_bots.add(username)
    await msg.reply(f"✅ @{username} добавлен в мониторинг")

@dp.message(Command('remove'))
async def cmd_remove(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /remove @username")
    username = args[1].lstrip('@')
    watched_bots.discard(username)
    await msg.reply(f"✅ @{username} удалён")

@dp.message(Command('list'))
async def cmd_list(msg: Message):
    if not watched_bots:
        return await msg.reply("📭 Список пуст")
    await msg.reply("🔍 Отслеживаемые:\n" + "\n".join(f"• @{b}" for b in sorted(watched_bots)))

@dp.message()
async def handle_username(msg: Message):
    text = msg.text.strip()
    if text.startswith('@'):
        username = text[1:]
        status_msg = await msg.reply(f"🔎 Проверяю @{username}...")
        success, rt, info = await quick_check(username)
        status = get_status(username)
        status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
        report = f"🤖 @{username}\n{status_emoji} Статус: {status.upper()}\n⚡ Время: {rt} мс\n\nДля деталей: /fullreport {text}"
        await status_msg.edit_text(report)

# ========== ФОНОВЫЙ МОНИТОРИНГ ==========
watched_bots = set()

async def monitor_loop():
    while True:
        if watched_bots:
            logger.info(f"Фоновый мониторинг {len(watched_bots)} ботов")
            for username in list(watched_bots):
                try:
                    await quick_check(username)
                except Exception as e:
                    logger.error(f"Ошибка @{username}: {e}")
                await asyncio.sleep(1)
        await asyncio.sleep(MONITOR_INTERVAL)

# ========== ЗАВЕРШЕНИЕ ==========
async def shutdown(poll_task, mon_task, keep_task):
    logger.info("Остановка...")
    keep_task.cancel()
    mon_task.cancel()
    poll_task.cancel()
    await asyncio.gather(poll_task, mon_task, keep_task, return_exceptions=True)
    await user_client.disconnect()
    await bot_client.session.close()

async def main():
    init_db()
    await bot_client.delete_webhook()
    
    # Подключаем Telethon с повторными попытками
    for attempt in range(3):
        try:
            await user_client.start()
            me = await user_client.get_me()
            logger.info(f"Telethon авторизован как @{me.username}")
            break
        except Exception as e:
            logger.error(f"Попытка {attempt+1}/3 подключения Telethon: {e}")
            if attempt == 2:
                raise
            await asyncio.sleep(5)
    
    # Запускаем keep-alive
    keep_task = asyncio.create_task(keep_alive_loop())
    poll_task = asyncio.create_task(dp.start_polling(bot_client))
    mon_task = asyncio.create_task(monitor_loop())
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(poll_task, mon_task, keep_task)))
    
    await asyncio.gather(poll_task, mon_task, keep_task)

if __name__ == "__main__":
    asyncio.run(main())
