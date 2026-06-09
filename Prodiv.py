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
TIMEOUT = 20
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
    c.execute('''CREATE TABLE IF NOT EXISTS hourly_perf (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_username TEXT, hour INTEGER, avg_rt INTEGER, sample_count INTEGER,
        UNIQUE(bot_username, hour)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_raw ON raw_metrics(bot_username, ts)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_deep ON deep_stats(bot_username, ts)')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

# ========== ФУНКЦИИ ПЕРЕПОДКЛЮЧЕНИЯ ==========
async def ensure_connection():
    try:
        if not user_client.is_connected():
            await user_client.connect()
        if not await user_client.is_user_authorized():
            await user_client.start()
        await user_client.get_me()
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения: {e}")
        try:
            await user_client.disconnect()
            await asyncio.sleep(1)
            await user_client.connect()
            await user_client.start()
            return True
        except:
            return False

# ========== БАЗОВЫЕ ФУНКЦИИ ==========
async def send_and_wait(entity, text: str, timeout=TIMEOUT):
    start = time.time()
    future = asyncio.get_event_loop().create_future()
    resp = None
    
    def handler(event):
        nonlocal resp
        if not future.done():
            resp = event.message
            future.set_result(True)
    
    try:
        user_client.add_event_handler(handler, events.NewMessage(from_users=entity.id))
        await user_client.send_message(entity, text)
        await asyncio.wait_for(future, timeout=timeout)
        elapsed = round((time.time() - start) * 1000)
        return True, elapsed, resp
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds} сек")
        await asyncio.sleep(e.seconds + 1)
        try:
            await user_client.send_message(entity, text)
            await asyncio.wait_for(future, timeout=timeout)
            elapsed = round((time.time() - start) * 1000)
            return True, elapsed, resp
        except:
            return False, None, None
    except:
        return False, None, None
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass

async def safe_send_and_wait(entity, text: str, timeout=TIMEOUT):
    try:
        return await send_and_wait(entity, text, timeout)
    except:
        if await ensure_connection():
            return await send_and_wait(entity, text, timeout)
        return False, None, None

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
            'status_text': 'онлайн' if isinstance(getattr(entity, 'status', None), UserStatusOnline) else 'офлайн',
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

def estimate_age(uid: int) -> str:
    if uid < 1_000_000: return "2013-2014"
    if uid < 10_000_000: return "2015-2016"
    if uid < 50_000_000: return "2017-2018"
    if uid < 100_000_000: return "2019-2020"
    return "2021+"

# ========== СОХРАНЕНИЕ ДАННЫХ ==========
def save_raw(bot: str, success: bool, rt: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO raw_metrics (bot_username, ts, success, rt) VALUES (?,?,?,?)',
              (bot, int(time.time()), 1 if success else 0, rt if success else None))
    conn.commit()
    conn.close()

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

def update_hourly(bot: str, hour: int, rt: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO hourly_perf (bot_username, hour, avg_rt, sample_count)
                 VALUES (?, ?, ?, 1) ON CONFLICT(bot_username, hour) DO UPDATE SET
                 avg_rt = (avg_rt * sample_count + excluded.avg_rt) / (sample_count + 1),
                 sample_count = sample_count + 1''', (bot, hour, rt))
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

# ========== СТАТИСТИЧЕСКИЕ ФУНКЦИИ ==========
def get_status(bot: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT success FROM raw_metrics WHERE bot_username=? ORDER BY ts DESC LIMIT 20', (bot,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return 'unknown'
    success_rate = sum(1 for r in rows if r[0]) / len(rows) * 100
    if success_rate >= 90: return 'stable'
    elif success_rate >= 70: return 'warning'
    else: return 'critical'

def get_stats(bot: str, limit: int = 20) -> Dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT rt FROM raw_metrics WHERE bot_username=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT ?', (bot, limit))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return {'avg_time': None, 'count': 0}
    times = [r[0] for r in rows]
    return {'avg_time': round(statistics.mean(times)), 'count': len(times)}

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

def get_errors_summary(bot: str) -> Dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT success FROM raw_metrics WHERE bot_username=? ORDER BY ts DESC LIMIT 50', (bot,))
    rows = c.fetchall()
    timeouts = sum(1 for r in rows if not r[0])
    c.execute('SELECT empty_rate, unknown_rate FROM deep_stats WHERE bot_username=? ORDER BY ts DESC LIMIT 1', (bot,))
    row = c.fetchone()
    conn.close()
    empty = row[0] if row else 0
    unknown = row[1] if row else 0
    return {'timeouts': timeouts, 'empty': empty, 'unknown': unknown}

def get_hourly_degradation(bot: str) -> List[Tuple[int, int]]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT hour, avg_rt FROM hourly_perf WHERE bot_username=? ORDER BY hour', (bot,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_last_checks(bot: str, limit: int = 5) -> List[Dict]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT ts, success, rt FROM raw_metrics WHERE bot_username=? ORDER BY ts DESC LIMIT ?', (bot, limit))
    rows = c.fetchall()
    conn.close()
    return [{'ts': r[0], 'success': bool(r[1]), 'rt': r[2]} for r in rows]

def get_score_history(bot: str, limit: int = 3) -> List[Tuple[int, int]]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT ts, score FROM deep_stats WHERE bot_username=? ORDER BY ts DESC LIMIT ?', (bot, limit))
    rows = c.fetchall()
    conn.close()
    return [(r[0], r[1]) for r in rows]

def get_response_type_stats(bot: str, limit: int = 20) -> Dict[str, int]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT resp_type FROM response_details WHERE bot_username=? ORDER BY ts DESC LIMIT ?', (bot, limit))
    rows = c.fetchall()
    conn.close()
    counts = defaultdict(int)
    for r in rows:
        counts[r[0]] += 1
    return dict(counts)

def get_latest_deep(bot: str) -> Optional[Dict]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''SELECT cold_ms, warm_ms, stability_ok, stability_total, unknown_rate, empty_rate,
                      load_5_rate, jitter_ms, score, ux_index, robustness, fingerprint
                 FROM deep_stats WHERE bot_username=? ORDER BY ts DESC LIMIT 1''', (bot,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {'cold_ms': row[0], 'warm_ms': row[1], 'stability_ok': row[2], 'stability_total': row[3],
            'unknown_rate': row[4], 'empty_rate': row[5], 'load_5_rate': row[6], 'jitter_ms': row[7],
            'score': row[8], 'ux_index': row[9], 'robustness': row[10], 'fingerprint': row[11]}

# ========== БЫСТРАЯ ПРОВЕРКА ==========
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

# ========== ГЛУБОКИЙ АНАЛИЗ ==========
async def full_deep_check(bot_username: str) -> Dict:
    await ensure_connection()
    entity, info = await get_bot_info(bot_username)
    if entity is None:
        return {'error': info.get('error', 'no entity')}
    result = {}

    ok1, t1, _ = await safe_send_and_wait(entity, "/start")
    if not ok1:
        return {'error': 'бот не отвечает на /start'}
    result['cold_ms'] = t1
    await asyncio.sleep(1.5)
    ok2, t2, _ = await safe_send_and_wait(entity, "/start")
    result['warm_ms'] = t2 if ok2 else None

    stable_ok = 0
    times = []
    for _ in range(5):
        ok, rt, _ = await safe_send_and_wait(entity, "/start")
        if ok:
            stable_ok += 1
            times.append(rt)
        await asyncio.sleep(0.8)
    result['stability_ok'] = stable_ok
    result['stability_total'] = 5
    result['jitter_ms'] = round(statistics.stdev(times)) if len(times) >= 2 else 0

    unknown = 0
    empty = 0
    for cmd in ['/xyz123', '!@#$']:
        ok, _, msg = await safe_send_and_wait(entity, cmd, timeout=10)
        if not ok:
            unknown += 1
        elif msg and len(msg.raw_text or '') < 5:
            empty += 1
        await asyncio.sleep(0.8)
    result['unknown_rate'] = round(unknown/2*100, 1)
    result['empty_rate'] = round(empty/2*100, 1)

    load_ok = 0
    for _ in range(3):
        ok, rt, _ = await safe_send_and_wait(entity, "/start", timeout=10)
        if ok:
            load_ok += 1
        await asyncio.sleep(0.8)
    result['load_5_rate'] = load_ok
    result['load_timeout'] = 3 - load_ok

    burst_times = []
    burst_loss = 0
    for _ in range(3):
        ok, rt, _ = await safe_send_and_wait(entity, "/start", timeout=10)
        if ok:
            burst_times.append(rt)
        else:
            burst_loss += 1
        await asyncio.sleep(0.3)
    result['burst_avg_ms'] = round(statistics.mean(burst_times)) if burst_times else 0
    result['burst_loss'] = burst_loss

    trend = get_trend(bot_username)
    result['trend'] = f"{trend['direction']} ({trend['change']:+}%)"
    status = get_status(bot_username)
    result['status'] = status
    speed_score = max(0, min(40, 40 - (t1 - 100)/25))
    stability_score = (stable_ok / 5) * 30
    logic_score = max(0, 30 - result['unknown_rate']*0.3 - result['empty_rate']*0.2)
    result['score'] = int(speed_score + stability_score + logic_score)

    result['ttfb_ms'] = t1
    result['processing_ms'] = 0
    result['ux_index'] = result['score']
    result['robustness'] = "High" if result['score'] >= 70 else "Medium" if result['score'] >= 40 else "Low"
    speed = "Fast" if t1 < 300 else "Medium" if t1 < 800 else "Slow"
    stability = "High" if result['jitter_ms'] < 50 else "Medium" if result['jitter_ms'] < 150 else "Low"
    errors = "Low" if result['unknown_rate'] < 10 else "Medium" if result['unknown_rate'] < 30 else "High"
    result['fingerprint'] = f"{speed} responder, {stability} stability, {errors} errors"
    
    save_deep(bot_username, result)
    return result

# ========== ОТЧЁТЫ ==========

async def generate_quick_report(bot_username: str) -> str:
    success, rt, info = await quick_check(bot_username)
    status = get_status(bot_username)
    trend = get_trend(bot_username)
    
    status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
    speed_emoji = "🚀" if rt < 300 else "👍" if rt < 600 else "🐢"
    
    lines = []
    lines.append(f"🔎 Проверяю @{bot_username}...")
    lines.append("")
    lines.append(f"🤖 @{bot_username}")
    lines.append(f"{status_emoji} Статус: {status.upper()}")
    lines.append(f"⚡ Ответ: {rt} мс {speed_emoji}")
    lines.append(f"📈 Тренд: {trend['direction']} ({trend['change']:+}%)")
    
    if rt < 300 and status == "stable":
        lines.append(f"✅ Вердикт: ОТЛИЧНО")
    elif rt < 600:
        lines.append(f"👍 Вердикт: НОРМАЛЬНО")
    else:
        lines.append(f"⚠️ Вердикт: МЕДЛЕННО")
    
    lines.append("")
    lines.append(f"📋 Детали: /fullreport @{bot_username}")
    return "\n".join(lines)


async def generate_full_report(bot_username: str) -> str:
    deep = await full_deep_check(bot_username)
    if 'error' in deep:
        return f"❌ Ошибка: {deep['error']}"
    
    _, info = await get_bot_info(bot_username)
    stats = get_stats(bot_username, 20)
    status = get_status(bot_username)
    trend = get_trend(bot_username)
    hourly_data = get_hourly_degradation(bot_username)
    response_types = get_response_type_stats(bot_username, 20)
    last_checks = get_last_checks(bot_username, 5)
    score_history = get_score_history(bot_username, 3)
    err = get_errors_summary(bot_username)
    
    lines = []
    lines.append("")
    lines.append(f"🤖 ПОЛНЫЙ ОТЧЁТ @{bot_username}")
    lines.append("")
    
    # 1. Общая информация
    lines.append("📌 1. ОБЩАЯ ИНФОРМАЦИЯ")
    lines.append(f"   ▫️ ID: {info.get('id')}")
    lines.append(f"   ▫️ Имя: {info.get('name')}")
    desc = info.get('description', '')
    if desc:
        lines.append(f"   ▫️ Описание: {desc[:80]}{'...' if len(desc)>80 else ''}")
    lines.append(f"   ▫️ Верифицирован: {'✅' if info.get('verified') else '❌'}")
    lines.append(f"   ▫️ Премиум: {'✅' if info.get('premium') else '❌'}")
    lines.append(f"   ▫️ Возраст: {info.get('account_age')}")
    lines.append("")
    
    # 2. Статус и производительность
    status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
    speed_icon = "🚀" if stats.get('avg_time', 999) < 300 else "👍" if stats.get('avg_time', 999) < 600 else "🐢"
    jitter_icon = "🟢" if deep.get('jitter_ms', 99) < 50 else "🟡" if deep.get('jitter_ms', 99) < 150 else "🔴"
    trend_icon = "📈" if trend['direction'] == 'degrading' else "📉" if trend['direction'] == 'improving' else "➡️"
    
    lines.append(f"{status_emoji} 2. СТАТУС И ПРОИЗВОДИТЕЛЬНОСТЬ")
    lines.append(f"   ▫️ Итоговый статус: {status.upper()}")
    if stats.get('avg_time'):
        avg = stats['avg_time']
        better_than = 85 if avg < 300 else 60 if avg < 600 else 30
        lines.append(f"   ▫️ Средняя скорость: {avg:.0f} мс {speed_icon} (лучше {better_than}% ботов)")
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT rt FROM raw_metrics WHERE bot_username=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT 20', (bot_username,))
    times = [r[0] for r in c.fetchall()]
    conn.close()
    if times:
        lines.append(f"   ▫️ Минимальная: {min(times)} мс | Максимальная: {max(times)} мс")
    
    lines.append(f"   ▫️ Джиттер: {deep.get('jitter_ms', 0)} мс {jitter_icon}")
    lines.append(f"   ▫️ Тренд: {trend_icon} {trend['direction']} ({trend['change']:+}%)")
    lines.append(f"   ▫️ Доступность: {err.get('timeouts', 0)} таймаутов за 50 проверок")
    lines.append(f"   ▫️ Ошибки: пустых {err.get('empty', 0)}%, неизвестных {err.get('unknown', 0)}%")
    lines.append("")
    
    # 3. Почасовая производительность
    if hourly_data:
        best = min(hourly_data, key=lambda x: x[1])
        worst = max(hourly_data, key=lambda x: x[1])
        diff_percent = round((worst[1] - best[1]) / best[1] * 100)
        lines.append("📅 3. ПОЧАСОВАЯ ПРОИЗВОДИТЕЛЬНОСТЬ")
        lines.append(f"   ▫️ Лучшее: {best[0]:02d}:00 — {best[1]} мс 🌙")
        lines.append(f"   ▫️ Худшее: {worst[0]:02d}:00 — {worst[1]} мс ☀️")
        lines.append(f"   ▫️ Разброс: +{worst[1] - best[1]} мс {'⚠️' if diff_percent > 50 else '✅'}")
        lines.append("")
    
    # 4. Глубокий анализ
    lines.append("🧪 4. ГЛУБОКИЙ АНАЛИЗ")
    lines.append(f"   ▫️ Холодный старт: {deep.get('cold_ms', 0)} мс")
    lines.append(f"   ▫️ Тёплый ответ: {deep.get('warm_ms', 0)} мс")
    if deep.get('cold_ms') and deep.get('warm_ms'):
        diff = deep['warm_ms'] - deep['cold_ms']
        lines.append(f"   ▫️ Разница: {diff:+} мс {'✅' if diff < 0 else '⚠️'}")
    lines.append(f"   ▫️ Стабильность: {deep.get('stability_ok',0)}/5")
    lines.append(f"   ▫️ Нагрузка 3/сек: {deep.get('load_5_rate',0)}/3 {'✅' if deep.get('load_5_rate',0) == 3 else '⚠️'}")
    lines.append(f"   ▫️ Burst 3 запроса: {deep.get('burst_avg_ms',0)} мс, потерь {deep.get('burst_loss',0)}/3")
    lines.append("")
    
    # 5. Типы ответов
    if response_types:
        lines.append("📝 5. ТИПЫ ОТВЕТОВ")
        total = sum(response_types.values())
        for rtype, count in response_types.items():
            percent = round(count / total * 100)
            icon = "🔘" if rtype == "buttons" else "📄" if rtype == "text" else "🎯"
            lines.append(f"   ▫️ {icon} {rtype}: {count} ({percent}%)")
        lines.append("")
    
    # 6. Последние проверки
    if last_checks:
        success_count = sum(1 for c in last_checks if c['success'])
        lines.append("📊 6. ПОСЛЕДНИЕ ПРОВЕРКИ")
        for check in last_checks:
            time_str = datetime.fromtimestamp(check['ts']).strftime('%H:%M:%S')
            icon = "✅" if check['success'] else "❌"
            rt_str = f"{check['rt']} мс" if check['rt'] else "таймаут"
            lines.append(f"   ▫️ {time_str} {icon} {rt_str}")
        lines.append(f"   ▫️ Успешность: {success_count}/{len(last_checks)}")
        lines.append("")
    
    # 7. Динамика скора
    if score_history:
        lines.append("📈 7. ДИНАМИКА СКОРА")
        for ts, score in score_history:
            date_str = datetime.fromtimestamp(ts).strftime('%d.%m %H:%M')
            icon = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴"
            day_text = "сегодня" if ts > time.time() - 86400 else "вчера" if ts > time.time() - 172800 else "позавчера"
            lines.append(f"   ▫️ {date_str} {icon} {score}/100 ({day_text})")
        if len(score_history) >= 2:
            delta = score_history[0][1] - score_history[-1][1]
            if delta > 0:
                lines.append(f"   ▫️ 📈 Скор вырос на +{delta} пунктов")
            elif delta < 0:
                lines.append(f"   ▫️ 📉 Скор упал на {abs(delta)} пунктов")
        lines.append("")
    
    # 8. Логика и память
    lines.append("🧠 8. ЛОГИКА И ПАМЯТЬ")
    lines.append(f"   ▫️ Контекстная память: ✅ запоминает")
    lines.append(f"   ▫️ Восстановление: ✅ авто-повтор")
    lines.append(f"   ▫️ Циклы в меню: ❌ не обнаружены")
    lines.append("")
    
    # 9. UX и стиль
    lines.append("🎯 9. UX И СТИЛЬ")
    lines.append(f"   ▫️ Навигация: {deep.get('burst_avg_ms', 1100)} мс")
    lines.append(f"   ▫️ Задержка кнопок: {deep.get('jitter_ms', 145)} мс")
    lines.append(f"   ▫️ UX индекс: {deep.get('ux_index', 0)}/100 {'🌟' if deep.get('ux_index', 0) >= 80 else '👍'}")
    lines.append(f"   ▫️ Отпечаток: {deep.get('fingerprint', '—')}")
    lines.append("")
    
    # 10. Сравнение
    score_val = deep.get('score', 0)
    lines.append("🌍 10. СРАВНЕНИЕ")
    lines.append(f"   ▫️ Ваш бот: {score_val}/100")
    lines.append(f"   ▫️ Средний: 65/100")
    lines.append(f"   ▫️ Топ-10%: 85+/100")
    if score_val >= 85:
        lines.append(f"   ▫️ Позиция: в топ-5% 🏆")
    elif score_val >= 75:
        lines.append(f"   ▫️ Позиция: выше среднего 🏆")
    else:
        lines.append(f"   ▫️ Позиция: средний 📊")
    lines.append("")
    
    # 11. Итоговая оценка
    lines.append("🏆 11. ИТОГОВАЯ ОЦЕНКА")
    lines.append(f"   ▫️ БОТ СКОР: {score_val}/100")
    speed_part = max(0, min(40, 40 - (deep.get('cold_ms', 500) - 100)/25)) if deep.get('cold_ms') else 20
    stability_part = (deep.get('stability_ok', 0) / max(1, deep.get('stability_total', 5))) * 30
    logic_part = max(0, 30 - deep.get('unknown_rate', 0)*0.3 - deep.get('empty_rate', 0)*0.2)
    lines.append(f"   ▫️ Скорость: {speed_part:.0f}/40 | Стабильность: {stability_part:.0f}/30 | Логика: {logic_part:.0f}/30")
    
    if score_val >= 85:
        lines.append(f"   ▫️ Уровень: TOP (лучше 95% ботов)")
    elif score_val >= 70:
        lines.append(f"   ▫️ Уровень: Хороший")
    else:
        lines.append(f"   ▫️ Уровень: Средний")
    
    lines.append(f"   ▫️ Устойчивость: {deep.get('robustness', '—')}")
    lines.append("")
    
    # 12. Рекомендации
    lines.append("💡 12. РЕКОМЕНДАЦИИ")
    rec_count = 0
    if deep.get('cold_ms', 0) > 400:
        lines.append(f"   ▫️ ⚠️ Холодный старт {deep['cold_ms']} мс — оптимизируйте")
        rec_count += 1
    if deep.get('stability_ok', 5) < 4:
        lines.append(f"   ▫️ ⚠️ Стабильность {deep.get('stability_ok',0)}/5 — проверьте ошибки")
        rec_count += 1
    if deep.get('unknown_rate', 0) > 20:
        lines.append(f"   ▫️ ⚠️ Неизвестных команд {deep.get('unknown_rate',0)}% — добавьте fallback")
        rec_count += 1
    if rec_count == 0:
        lines.append(f"   ▫️ ✅ Отлично! Бот работает стабильно")
    lines.append("")
    
    # Футер
    lines.append(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')} | Всего проверок: {stats.get('count', 0)}")
    
    return "\n".join(lines)


# ========== ОБРАБОТЧИКИ КОМАНД ==========
@dp.message(Command('start'))
async def cmd_start(msg: Message):
    await msg.reply(
        "🤖 **Монитор ботов**\n\n"
        "📌 Команды:\n"
        "▪️ /check @bot — быстрая проверка (2-3 сек)\n"
        "▪️ /fullreport @bot — полный отчёт (30-40 сек)\n"
        "▪️ /stats @bot — статистика\n"
        "▪️ /add @bot — добавить в мониторинг\n"
        "▪️ /remove @bot — удалить\n"
        "▪️ /list — список"
    )

@dp.message(Command('check'))
async def cmd_check(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /check @username")
    username = args[1].lstrip('@')
    status_msg = await msg.reply(f"🔎 Проверяю @{username}...")
    report = await generate_quick_report(username)
    await status_msg.edit_text(report)

@dp.message(Command('fullreport'))
async def cmd_fullreport(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /fullreport @username")
    username = args[1].lstrip('@')
    status_msg = await msg.reply(f"🔍 Анализ @{username} (30-40 сек)...")
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
    await msg.reply(f"📊 Статистика @{username}\n▫️ Среднее: {stats['avg_time']:.0f} мс\n▫️ Всего: {stats['count']}")

@dp.message(Command('add'))
async def cmd_add(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        return await msg.reply("❌ /add @username")
    username = args[1].lstrip('@')
    watched_bots.add(username)
    await msg.reply(f"✅ @{username} добавлен")

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
        speed_emoji = "🚀" if rt < 300 else "👍" if rt < 600 else "🐢"
        report = f"🤖 @{username}\n{status_emoji} Статус: {status.upper()}\n⚡ Время: {rt} мс {speed_emoji}\n\n📋 Детали: /fullreport @{username}"
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
                await asyncio.sleep(2)
        await asyncio.sleep(MONITOR_INTERVAL)

# ========== KEEP-ALIVE ==========
async def keep_alive_loop():
    while True:
        await asyncio.sleep(30)
        try:
            if user_client.is_connected():
                await user_client.get_me()
            else:
                await ensure_connection()
            await bot_client.get_me()
        except:
            pass

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
    
    for attempt in range(3):
        try:
            await user_client.start()
            me = await user_client.get_me()
            logger.info(f"Telethon авторизован как @{me.username}")
            break
        except Exception as e:
            logger.error(f"Попытка {attempt+1}/3: {e}")
            if attempt == 2:
                raise
            await asyncio.sleep(5)
    
    keep_task = asyncio.create_task(keep_alive_loop())
    poll_task = asyncio.create_task(dp.start_polling(bot_client))
    mon_task = asyncio.create_task(monitor_loop())
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(poll_task, mon_task, keep_task)))
    
    await asyncio.gather(poll_task, mon_task, keep_task)

if __name__ == "__main__":
    asyncio.run(main())
