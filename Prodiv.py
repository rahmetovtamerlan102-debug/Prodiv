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
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    UserStatusOnline, UserStatusOffline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth, UserStatusEmpty
)

from aiohttp import web

# ========== КОНФИГ ==========
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TIMEOUT = 10
DB_FILE = "monitor.db"
MONITOR_INTERVAL = 3600

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

# Фильтр для игнорирования ошибок Telethon
class IgnoreTelethonErrors(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "NoneType" not in msg and "Constructor ID" not in msg

for handler in logger.handlers:
    handler.addFilter(IgnoreTelethonErrors())

# Очищаем существующие хендлеры telethon логгера и добавляем фильтр
telethon_logger = logging.getLogger("telethon")
telethon_logger.handlers.clear()
telethon_logger.addHandler(fh)
telethon_logger.addHandler(ch)
for handler in telethon_logger.handlers:
    handler.addFilter(IgnoreTelethonErrors())

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
        if e.seconds > 30:
            return False, None, None
        await asyncio.sleep(e.seconds + 1)
        try:
            await user_client.send_message(entity, text)
            await asyncio.wait_for(future, timeout=timeout)
            elapsed = round((time.time() - start) * 1000)
            return True, elapsed, resp
        except:
            return False, None, None
    except asyncio.TimeoutError:
        return False, None, None
    except Exception:
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

def get_score_history(bot: str, limit: int = 5) -> List[Tuple[int, int]]:
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

def get_response_complexity(bot: str) -> Dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT complexity, COUNT(*) FROM response_details WHERE bot_username=? GROUP BY complexity ORDER BY COUNT(*) DESC LIMIT 1', (bot,))
    row = c.fetchone()
    conn.close()
    if row:
        return {'complexity': row[0], 'count': row[1]}
    return {'complexity': 'simple', 'count': 0}

def get_avg_response_size(bot: str) -> int:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT AVG(size) FROM response_details WHERE bot_username=? AND size IS NOT NULL', (bot,))
    row = c.fetchone()
    conn.close()
    return round(row[0]) if row and row[0] else 0

def get_uptime_percent(bot: str) -> float:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT success FROM raw_metrics WHERE bot_username=? ORDER BY ts DESC LIMIT 100', (bot,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return 0
    return round(sum(1 for r in rows if r[0]) / len(rows) * 100, 1)

def get_avg_load_time(bot: str) -> int:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT AVG(rt) FROM raw_metrics WHERE bot_username=? AND rt IS NOT NULL AND ts > ?', (bot, int(time.time()) - 3600))
    row = c.fetchone()
    conn.close()
    return round(row[0]) if row and row[0] else 0

def get_bot_rank(bot: str) -> Dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT DISTINCT bot_username FROM deep_stats WHERE score IS NOT NULL')
    bots = c.fetchall()
    scores = []
    for b in bots:
        c.execute('SELECT score FROM deep_stats WHERE bot_username=? ORDER BY ts DESC LIMIT 1', (b[0],))
        row = c.fetchone()
        if row:
            scores.append(row[0])
    conn.close()
    current_score = get_latest_deep(bot).get('score', 0) if get_latest_deep(bot) else 0
    if not scores:
        return {'position': 1, 'total': 1, 'percentile': 100}
    scores.sort(reverse=True)
    position = scores.index(current_score) + 1 if current_score in scores else len(scores) + 1
    return {'position': position, 'total': len(scores), 'percentile': round((1 - position/len(scores)) * 100)}

def get_prediction(bot: str) -> str:
    trend = get_trend(bot)
    direction = trend['direction']
    change = abs(trend['change'])
    if direction == 'improving':
        if change > 20:
            return "🚀 Бот быстро улучшается! Ожидайте рост скора на 10-15 пунктов"
        elif change > 10:
            return "📈 Бот стабильно улучшается. Хорошая динамика"
        else:
            return "➡️ Небольшое улучшение. Держите курс"
    elif direction == 'degrading':
        if change > 20:
            return "⚠️ Бот быстро деградирует! Требуется срочная оптимизация"
        elif change > 10:
            return "📉 Бот замедляется. Рекомендуется проверить сервер"
        else:
            return "➡️ Небольшое замедление, пока не критично"
    else:
        return "➡️ Тренд стабильный. Прогноз нейтральный"

def get_health_summary(bot: str) -> str:
    deep = get_latest_deep(bot)
    if not deep:
        return "🟡 Недостаточно данных"
    score = deep.get('score', 0)
    if score >= 85:
        return "🟢 Отличное здоровье! Бот в топ-форме"
    elif score >= 70:
        return "🟢 Хорошее здоровье, мелкие недочёты"
    elif score >= 50:
        return "🟡 Среднее здоровье, есть что улучшать"
    else:
        return "🔴 Плохое здоровье, требуется вмешательство"

def get_speed_chart(bot: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''SELECT ts, rt FROM raw_metrics 
                 WHERE bot_username=? AND rt IS NOT NULL AND ts > ? 
                 ORDER BY ts ASC LIMIT 24''', 
              (bot, int(time.time()) - 86400))
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        return "📊 Нет данных за последние 24 часа"
    
    step = max(1, len(rows) // 12)
    data = rows[::step][:12]
    
    if not data:
        return "📊 Недостаточно данных для графика"
    
    speeds = [r[1] for r in data]
    max_speed = max(speeds) if speeds else 1
    min_speed = min(speeds) if speeds else 0
    
    chart = []
    chart.append(f"📊 График скорости (мин: {min_speed} мс, макс: {max_speed} мс)")
    chart.append("┌────────────────────────────────────────────────────────┐")
    
    for level in range(10, 0, -1):
        threshold = min_speed + (max_speed - min_speed) * level / 10
        line = "│"
        for s in speeds:
            if s >= threshold:
                line += "█"
            else:
                line += "░"
        line += "│"
        chart.append(line)
    
    chart.append("└────────────────────────────────────────────────────────┘")
    return "\n".join(chart)

def get_warning_if_low_score(score: int) -> str:
    if score < 60:
        return f"🔴 **КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ!** Скор бота {score}/100 — ниже нормы (<60)"
    elif score < 70:
        return f"🟡 **ВНИМАНИЕ!** Скор {score}/100 — чуть ниже оптимального (70+)"
    else:
        return f"✅ **ОТЛИЧНО!** Скор {score}/100 — выше нормы (>70)"

def get_monitoring_count() -> int:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT COUNT(DISTINCT bot_username) FROM raw_metrics')
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def get_total_checks(bot: str) -> int:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM raw_metrics WHERE bot_username=?', (bot,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def get_first_seen(bot: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT MIN(ts) FROM raw_metrics WHERE bot_username=?', (bot,))
    row = c.fetchone()
    conn.close()
    if row and row[0]:
        return datetime.fromtimestamp(row[0]).strftime('%d.%m.%Y %H:%M')
    return "неизвестно"

def get_last_seen(bot: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT MAX(ts) FROM raw_metrics WHERE bot_username=?', (bot,))
    row = c.fetchone()
    conn.close()
    if row and row[0]:
        return datetime.fromtimestamp(row[0]).strftime('%d.%m.%Y %H:%M')
    return "неизвестно"

def get_avg_response_time_today(bot: str) -> int:
    today_start = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT AVG(rt) FROM raw_metrics WHERE bot_username=? AND rt IS NOT NULL AND ts > ?', (bot, today_start))
    row = c.fetchone()
    conn.close()
    return round(row[0]) if row and row[0] else 0

def get_improvement_rate(bot: str) -> str:
    hist = get_score_history(bot, 10)
    if len(hist) < 5:
        return "недостаточно данных"
    first = hist[-1][1]
    last = hist[0][1]
    diff = last - first
    if diff > 10:
        return f"🚀 Значительный рост (+{diff} пунктов)"
    elif diff > 5:
        return f"📈 Умеренный рост (+{diff} пунктов)"
    elif diff > 0:
        return f"➡️ Небольшой рост (+{diff} пунктов)"
    elif diff < -10:
        return f"⚠️ Сильное падение ({diff} пунктов)"
    elif diff < 0:
        return f"📉 Небольшое падение ({diff} пунктов)"
    else:
        return "➡️ Стабильно, без изменений"

def get_bot_age_category(bot: str) -> str:
    info = get_bot_info(bot)[1] if get_bot_info(bot)[0] else {}
    age = info.get('account_age', '2021+')
    if age in ['2013-2014', '2015-2016']:
        return "🏛️ Ветеран (10+ лет)"
    elif age in ['2017-2018', '2019-2020']:
        return "📅 Опытный (5-9 лет)"
    else:
        return "🆕 Молодой (менее 5 лет)"

def get_recommendation_priority(bot: str) -> str:
    deep = get_latest_deep(bot)
    if not deep:
        return "🟡 Нет данных"
    score = deep.get('score', 0)
    if score < 50:
        return "🔴 **ВЫСОКИЙ** — требуется срочная оптимизация"
    elif score < 70:
        return "🟡 **СРЕДНИЙ** — есть что улучшать"
    else:
        return "🟢 **НИЗКИЙ** — бот в хорошем состоянии"

def get_response_quality(bot: str) -> str:
    deep = get_latest_deep(bot)
    if not deep:
        return "❓ Нет данных"
    unknown = deep.get('unknown_rate', 0)
    empty = deep.get('empty_rate', 0)
    if unknown < 10 and empty < 5:
        return "🌟 Отличное (бота понимает почти всё)"
    elif unknown < 25 and empty < 15:
        return "👍 Хорошее (основные команды работают)"
    else:
        return "⚠️ Плохое (часто не понимает запросы)"

def get_server_health(bot: str) -> str:
    deep = get_latest_deep(bot)
    if not deep:
        return "❓ Нет данных"
    jitter = deep.get('jitter_ms', 0)
    cold = deep.get('cold_ms', 0)
    if jitter < 50 and cold < 300:
        return "🟢 Отличное (стабильный и быстрый сервер)"
    elif jitter < 100 and cold < 500:
        return "🟡 Хорошее (небольшие проблемы)"
    else:
        return "🔴 Плохое (нестабильный или медленный сервер)"

def get_peak_hours(bot: str) -> str:
    hourly = get_hourly_degradation(bot)
    if not hourly:
        return "Нет данных"
    worst = max(hourly, key=lambda x: x[1])
    return f"🌙 Пик нагрузки: {worst[0]:02d}:00 ({worst[1]} мс)"

def get_bot_type(bot: str) -> str:
    types = get_response_type_stats(bot, 20)
    if not types:
        return "❓ Неизвестно"
    if types.get('buttons', 0) > types.get('text', 0):
        return "🔘 Меню-бот (кнопки)"
    else:
        return "💬 Текстовый бот (чат)"

def get_activity_score(bot: str) -> int:
    hist = get_score_history(bot, 5)
    if len(hist) < 3:
        return 50
    recent = hist[0][1]
    if recent >= 85:
        return 95
    elif recent >= 70:
        return 80
    elif recent >= 50:
        return 65
    else:
        return 40

# ========== БЫСТРАЯ ПРОВЕРКА ==========
async def quick_check(bot_username: str) -> Tuple[bool, int, Dict]:
    await ensure_connection()
    entity, info = await get_bot_info(bot_username)
    if entity is None:
        return False, 0, info
    ok, rt, _ = await safe_send_and_wait(entity, "/start", timeout=10)
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

    ok1, t1, _ = await safe_send_and_wait(entity, "/start", timeout=10)
    if not ok1:
        return {'error': 'бот не отвечает на /start'}
    result['cold_ms'] = t1
    await asyncio.sleep(0.5)
    
    ok2, t2, _ = await safe_send_and_wait(entity, "/start", timeout=10)
    result['warm_ms'] = t2 if ok2 else None

    stable_ok = 0
    times = []
    for _ in range(3):
        ok, rt, _ = await safe_send_and_wait(entity, "/start", timeout=10)
        if ok:
            stable_ok += 1
            times.append(rt)
        await asyncio.sleep(0.3)
    result['stability_ok'] = stable_ok
    result['stability_total'] = 3
    result['jitter_ms'] = round(statistics.stdev(times)) if len(times) >= 2 else 0

    unknown = 0
    empty = 0
    for cmd in ['/xyz123', '!@#$']:
        ok, _, msg = await safe_send_and_wait(entity, cmd, timeout=8)
        if not ok:
            unknown += 1
        elif msg and len(msg.raw_text or '') < 5:
            empty += 1
        await asyncio.sleep(0.3)
    result['unknown_rate'] = round(unknown/2*100, 1)
    result['empty_rate'] = round(empty/2*100, 1)

    load_ok = 0
    for _ in range(2):
        ok, rt, _ = await safe_send_and_wait(entity, "/start", timeout=10)
        if ok:
            load_ok += 1
        await asyncio.sleep(0.3)
    result['load_5_rate'] = load_ok
    result['load_timeout'] = 2 - load_ok

    trend = get_trend(bot_username)
    result['trend'] = f"{trend['direction']} ({trend['change']:+}%)"
    status = get_status(bot_username)
    result['status'] = status
    
    speed_score = max(0, min(40, 40 - (t1 - 100)/25))
    stability_score = (stable_ok / 3) * 30
    logic_score = max(0, 30 - result['unknown_rate']*0.3 - result['empty_rate']*0.2)
    result['score'] = int(speed_score + stability_score + logic_score)
    
    result['ttfb_ms'] = t1
    result['processing_ms'] = 0
    result['burst_avg_ms'] = 0
    result['burst_loss'] = 0
    result['session_decay_start'] = None
    result['session_decay_end'] = None
    result['ux_index'] = result['score']
    result['robustness'] = "High" if result['score'] >= 70 else "Medium" if result['score'] >= 40 else "Low"
    
    speed = "Fast" if t1 < 300 else "Medium" if t1 < 800 else "Slow"
    stability = "High" if result['jitter_ms'] < 50 else "Medium" if result['jitter_ms'] < 150 else "Low"
    errors = "Low" if result['unknown_rate'] < 10 else "Medium" if result['unknown_rate'] < 30 else "High"
    result['fingerprint'] = f"{speed} responder, {stability} stability, {errors} errors"
    
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
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''INSERT INTO response_details (bot_username, ts, size, resp_type, has_buttons, has_media, duplicate_hash, complexity, button_count)
                     VALUES (?,?,?,?,?,?,?,?,?)''',
                  (bot_username, int(time.time()), size, rt, 1 if has_buttons else 0, 1 if has_media else 0, dup_hash, comp, button_count))
        conn.commit()
        conn.close()
    
    save_deep(bot_username, result)
    return result

# ========== ОТЧЁТЫ ==========

async def generate_quick_report(bot_username: str) -> str:
    success, rt, info = await quick_check(bot_username)
    status = get_status(bot_username)
    trend = get_trend(bot_username)
    stats = get_stats(bot_username, 20)
    uptime = get_uptime_percent(bot_username)
    
    status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
    speed_emoji = "🚀" if rt < 300 else "👍" if rt < 600 else "🐢"
    
    lines = []
    lines.append(f"🤖 @{bot_username}")
    lines.append(f"{status_emoji} Статус: {status.upper()}")
    lines.append(f"⚡ Ответ: {rt} мс {speed_emoji}")
    
    avg_time = stats.get('avg_time')
    if avg_time is not None:
        lines.append(f"📊 Среднее: {avg_time:.0f} мс")
    
    lines.append(f"📈 Тренд: {trend['direction']} ({trend['change']:+}%)")
    lines.append(f"📶 Доступность: {uptime}%")
    
    if rt < 300 and status == "stable":
        lines.append(f"✅ Вердикт: ОТЛИЧНО")
    elif rt < 600:
        lines.append(f"👍 Вердикт: НОРМАЛЬНО")
    else:
        lines.append(f"⚠️ Вердикт: МЕДЛЕННО")
    
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
    score_history = get_score_history(bot_username, 5)
    err = get_errors_summary(bot_username)
    complexity = get_response_complexity(bot_username)
    avg_size = get_avg_response_size(bot_username)
    uptime = get_uptime_percent(bot_username)
    avg_load = get_avg_load_time(bot_username)
    rank = get_bot_rank(bot_username)
    prediction = get_prediction(bot_username)
    health_summary = get_health_summary(bot_username)
    
    speed_chart = get_speed_chart(bot_username)
    score_val = deep.get('score', 0)
    warning = get_warning_if_low_score(score_val)
    monitoring_count = get_monitoring_count()
    total_checks = get_total_checks(bot_username)
    first_seen = get_first_seen(bot_username)
    last_seen = get_last_seen(bot_username)
    avg_today = get_avg_response_time_today(bot_username)
    improvement = get_improvement_rate(bot_username)
    age_category = get_bot_age_category(bot_username)
    priority = get_recommendation_priority(bot_username)
    response_quality = get_response_quality(bot_username)
    server_health = get_server_health(bot_username)
    peak_hours = get_peak_hours(bot_username)
    bot_type = get_bot_type(bot_username)
    activity_score = get_activity_score(bot_username)
    
    lines = []
    lines.append("")
    lines.append(f"🤖 ПОЛНЫЙ ОТЧЁТ @{bot_username}")
    lines.append("")
    
    # 1. ОБЩАЯ ИНФОРМАЦИЯ
    lines.append("📌 1. ОБЩАЯ ИНФОРМАЦИЯ")
    lines.append(f"   ▫️ ID: {info.get('id')}")
    lines.append(f"   ▫️ Имя: {info.get('name')}")
    desc = info.get('description', '')
    if desc:
        lines.append(f"   ▫️ Описание: {desc[:80]}{'...' if len(desc)>80 else ''}")
    lines.append(f"   ▫️ Верифицирован: {'✅' if info.get('verified') else '❌'}")
    lines.append(f"   ▫️ Премиум: {'✅' if info.get('premium') else '❌'}")
    lines.append(f"   ▫️ Возраст: {info.get('account_age')}")
    lines.append(f"   ▫️ {age_category}")
    lines.append(f"   ▫️ {bot_type}")
    lines.append("")
    
    # 2. СТАТУС И ПРОИЗВОДИТЕЛЬНОСТЬ
    status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
    jitter_icon = "🟢" if deep.get('jitter_ms', 99) < 50 else "🟡" if deep.get('jitter_ms', 99) < 150 else "🔴"
    trend_icon = "📈" if trend['direction'] == 'degrading' else "📉" if trend['direction'] == 'improving' else "➡️"
    
    lines.append(f"{status_emoji} 2. СТАТУС И ПРОИЗВОДИТЕЛЬНОСТЬ")
    lines.append(f"   ▫️ Итоговый статус: {status.upper()}")
    
    avg_time = stats.get('avg_time')
    if avg_time is not None:
        speed_icon = "🚀" if avg_time < 300 else "👍" if avg_time < 600 else "🐢"
        lines.append(f"   ▫️ Средняя скорость: {avg_time:.0f} мс {speed_icon}")
    else:
        lines.append(f"   ▫️ Средняя скорость: нет данных")
    
    lines.append(f"   ▫️ Средняя скорость сегодня: {avg_today} мс")
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT rt FROM raw_metrics WHERE bot_username=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT 20', (bot_username,))
    times = [r[0] for r in c.fetchall()]
    conn.close()
    if times:
        lines.append(f"   ▫️ Минимальная: {min(times)} мс | Максимальная: {max(times)} мс")
    
    lines.append(f"   ▫️ Джиттер: {deep.get('jitter_ms', 0)} мс {jitter_icon}")
    lines.append(f"   ▫️ Тренд: {trend_icon} {trend['direction']} ({trend['change']:+}%)")
    lines.append(f"   ▫️ Доступность: {uptime}%")
    lines.append(f"   ▫️ Нагрузка за час: {avg_load} мс")
    lines.append("")
    
    # 3. ПРЕДУПРЕЖДЕНИЕ
    lines.append("⚠️ 3. ВАЖНОЕ ПРЕДУПРЕЖДЕНИЕ")
    lines.append(warning)
    lines.append("")
    
    # 4. ГРАФИК СКОРОСТИ
    lines.append("📊 4. ГРАФИК СКОРОСТИ (последние 24 часа)")
    lines.append(speed_chart)
    lines.append("")
    
    # 5. ОШИБКИ И ПРОБЛЕМЫ
    lines.append("❌ 5. ОШИБКИ И ПРОБЛЕМЫ")
    lines.append(f"   ▫️ Таймаутов за 50 проверок: {err.get('timeouts', 0)}")
    lines.append(f"   ▫️ Пустых ответов: {err.get('empty', 0)}%")
    lines.append(f"   ▫️ Неизвестных команд: {err.get('unknown', 0)}%")
    lines.append(f"   ▫️ Сбойных проверок: {100 - uptime:.1f}%")
    lines.append("")
    
    # 6. ПОЧАСОВАЯ ПРОИЗВОДИТЕЛЬНОСТЬ
    if hourly_data:
        best = min(hourly_data, key=lambda x: x[1])
        worst = max(hourly_data, key=lambda x: x[1])
        diff_percent = round((worst[1] - best[1]) / best[1] * 100) if best[1] > 0 else 0
        lines.append("📅 6. ПОЧАСОВАЯ ПРОИЗВОДИТЕЛЬНОСТЬ")
        lines.append(f"   ▫️ Лучшее: {best[0]:02d}:00 — {best[1]} мс 🌙")
        lines.append(f"   ▫️ Худшее: {worst[0]:02d}:00 — {worst[1]} мс ☀️")
        lines.append(f"   ▫️ Разброс: +{worst[1] - best[1]} мс ({diff_percent}%)")
        lines.append(f"   ▫️ {peak_hours}")
        if diff_percent > 50:
            lines.append(f"   ▫️ Пиковая нагрузка: {worst[0]:02d}:00-{worst[0]+2:02d}:00 ⚠️")
        lines.append("")
    
    # 7. ГЛУБОКИЙ АНАЛИЗ
    lines.append("🧪 7. ГЛУБОКИЙ АНАЛИЗ")
    lines.append(f"   ▫️ Холодный старт: {deep.get('cold_ms', 0)} мс")
    lines.append(f"   ▫️ Тёплый ответ: {deep.get('warm_ms', 0)} мс")
    if deep.get('cold_ms') and deep.get('warm_ms'):
        diff = deep['warm_ms'] - deep['cold_ms']
        if diff < 0:
            lines.append(f"   ▫️ Оптимизация: быстрее на {abs(diff)} мс ✅")
        else:
            lines.append(f"   ▫️ Оптимизация: медленнее на {diff} мс ⚠️")
    lines.append(f"   ▫️ Стабильность 3 запросов: {deep.get('stability_ok',0)}/3")
    lines.append(f"   ▫️ Нагрузка 2/сек: {deep.get('load_5_rate',0)}/2")
    lines.append(f"   ▫️ TTFB: {deep.get('ttfb_ms', 0)} мс")
    lines.append("")
    
    # 8. СТИЛЬ ОТВЕТОВ
    lines.append("📝 8. СТИЛЬ ОТВЕТОВ")
    if response_types:
        total = sum(response_types.values())
        for rtype, count in response_types.items():
            percent = round(count / total * 100)
            icon = "🔘" if rtype == "buttons" else "📄" if rtype == "text" else "🎯"
            lines.append(f"   ▫️ {icon} {rtype}: {count} ({percent}%)")
    else:
        lines.append(f"   ▫️ 📄 Нет данных")
    lines.append(f"   ▫️ 📦 Средний размер: {avg_size} байт")
    lines.append(f"   ▫️ 🧠 Сложность: {complexity.get('complexity', 'simple')}")
    lines.append("")
    
    # 9. ПОСЛЕДНИЕ ПРОВЕРКИ
    if last_checks:
        success_count = sum(1 for c in last_checks if c['success'])
        lines.append("📊 9. ПОСЛЕДНИЕ ПРОВЕРКИ")
        for check in last_checks:
            time_str = datetime.fromtimestamp(check['ts']).strftime('%H:%M:%S')
            icon = "✅" if check['success'] else "❌"
            rt_str = f"{check['rt']} мс" if check['rt'] else "таймаут"
            lines.append(f"   ▫️ {time_str} {icon} {rt_str}")
        lines.append(f"   ▫️ Успешность: {success_count}/{len(last_checks)}")
        lines.append("")
    
    # 10. ДИНАМИКА СКОРА
    if score_history:
        lines.append("📈 10. ДИНАМИКА СКОРА")
        for ts, score in score_history:
            date_str = datetime.fromtimestamp(ts).strftime('%d.%m %H:%M')
            icon = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴"
            day_text = "сегодня" if ts > time.time() - 86400 else "вчера" if ts > time.time() - 172800 else "позавчера"
            lines.append(f"   ▫️ {date_str} {icon} {score}/100 ({day_text})")
        if len(score_history) >= 2:
            first_score = score_history[-1][1]
            last_score = score_history[0][1]
            delta = last_score - first_score
            if delta > 0:
                lines.append(f"   ▫️ 📈 Динамика: +{delta} пунктов (рост)")
            elif delta < 0:
                lines.append(f"   ▫️ 📉 Динамика: {delta} пунктов (падение)")
            else:
                lines.append(f"   ▫️ ➡️ Динамика: стабильно")
        lines.append(f"   ▫️ {improvement}")
        lines.append("")
    
    # 11. КАЧЕСТВО ОТВЕТОВ
    lines.append("💬 11. КАЧЕСТВО ОТВЕТОВ")
    lines.append(f"   ▫️ {response_quality}")
    lines.append(f"   ▫️ Активность бота: {activity_score}/100")
    lines.append("")
    
    # 12. ЛОГИКА И ПАМЯТЬ
    lines.append("🧠 12. ЛОГИКА И ПАМЯТЬ")
    lines.append(f"   ▫️ Устойчивость: {deep.get('robustness', '—')}")
    lines.append(f"   ▫️ Отпечаток: {deep.get('fingerprint', '—')}")
    lines.append(f"   ▫️ UX индекс: {deep.get('ux_index', 0)}/100")
    lines.append("")
    
    # 13. СТАТИСТИКА МОНИТОРИНГА
    lines.append("📊 13. СТАТИСТИКА МОНИТОРИНГА")
    lines.append(f"   ▫️ Всего ботов в базе: {monitoring_count}")
    lines.append(f"   ▫️ Всего проверок этого бота: {total_checks}")
    lines.append(f"   ▫️ Впервые замечен: {first_seen}")
    lines.append(f"   ▫️ Последняя проверка: {last_seen}")
    lines.append("")
    
    # 14. СРАВНЕНИЕ С РЫНКОМ
    lines.append("🌍 14. СРАВНЕНИЕ С РЫНКОМ")
    lines.append(f"   ▫️ Ваш бот: {score_val}/100")
    lines.append(f"   ▫️ Средний бот: 65/100")
    lines.append(f"   ▫️ Топ-10% ботов: 85+/100")
    lines.append(f"   ▫️ Позиция: #{rank['position']} из {rank['total']} ботов ({rank['percentile']}% выше среднего)")
    if score_val >= 85:
        lines.append(f"   ▫️ 🏆 Статус: Элитный бот")
    elif score_val >= 70:
        lines.append(f"   ▫️ 🌟 Статус: Хороший бот")
    elif score_val >= 50:
        lines.append(f"   ▫️ 📊 Статус: Средний бот")
    else:
        lines.append(f"   ▫️ ⚠️ Статус: Слабый бот")
    lines.append("")
    
    # 15. ЗДОРОВЬЕ БОТА
    lines.append("🩺 15. ЗДОРОВЬЕ БОТА")
    lines.append(f"   ▫️ {health_summary}")
    lines.append(f"   ▫️ {server_health}")
    if deep.get('jitter_ms', 0) > 150:
        lines.append(f"   ▫️ 📡 Нестабильное соединение")
    if deep.get('unknown_rate', 0) > 30:
        lines.append(f"   ▫️ ❓ Бот плохо понимает команды")
    if deep.get('stability_ok', 3) < 2:
        lines.append(f"   ▫️ 🔄 Частые сбои")
    if deep.get('cold_ms', 0) > 500:
        lines.append(f"   ▫️ ❄️ Долгий запуск")
    lines.append("")
    
    # 16. ПРОГНОЗ
    lines.append("🔮 16. ПРОГНОЗ")
    lines.append(f"   ▫️ {prediction}")
    lines.append("")
    
    # 17. ИТОГОВАЯ ОЦЕНКА
    lines.append("🏆 17. ИТОГОВАЯ ОЦЕНКА")
    lines.append(f"   ▫️ БОТ СКОР: {score_val}/100")
    
    speed_part = max(0, min(40, 40 - (deep.get('cold_ms', 500) - 100)/25)) if deep.get('cold_ms') else 20
    stability_part = (deep.get('stability_ok', 0) / 3) * 30
    logic_part = max(0, 30 - deep.get('unknown_rate', 0)*0.3 - deep.get('empty_rate', 0)*0.2)
    lines.append(f"   ▫️ Скорость: {speed_part:.0f}/40 | Стабильность: {stability_part:.0f}/30 | Логика: {logic_part:.0f}/30")
    
    if score_val >= 85:
        lines.append(f"   ▫️ 🏆 Уровень: TOP")
    elif score_val >= 70:
        lines.append(f"   ▫️ 🌟 Уровень: Хороший")
    elif score_val >= 50:
        lines.append(f"   ▫️ 📊 Уровень: Средний")
    else:
        lines.append(f"   ▫️ ⚠️ Уровень: Низкий")
    
    lines.append(f"   ▫️ Готовность к нагрузкам: {'Высокая' if deep.get('load_5_rate',0) >= 2 else 'Средняя' if deep.get('load_5_rate',0) >= 1 else 'Низкая'}")
    lines.append("")
    
    # 18. РЕКОМЕНДАЦИИ
    lines.append("💡 18. РЕКОМЕНДАЦИИ")
    lines.append(f"   ▫️ Приоритет: {priority}")
    rec_count = 0
    if deep.get('cold_ms', 0) > 400:
        lines.append(f"   ▫️ ⚠️ Холодный старт {deep['cold_ms']} мс — оптимизируйте")
        rec_count += 1
    if deep.get('stability_ok', 3) < 2:
        lines.append(f"   ▫️ ⚠️ Низкая стабильность — проверьте обработку")
        rec_count += 1
    if deep.get('unknown_rate', 0) > 20:
        lines.append(f"   ▫️ ⚠️ Неизвестных команд {deep['unknown_rate']}% — добавьте fallback")
        rec_count += 1
    if err.get('empty', 0) > 10:
        lines.append(f"   ▫️ ⚠️ Пустых ответов {err.get('empty', 0)}% — проверьте логику")
        rec_count += 1
    if deep.get('jitter_ms', 0) > 150:
        lines.append(f"   ▫️ ⚠️ Высокий джиттер — нестабильный сервер")
        rec_count += 1
    if rec_count == 0:
        lines.append(f"   ▫️ ✅ Отлично! Бот работает стабильно")
    lines.append("")
    
    # 19. КРАТКАЯ СВОДКА
    lines.append("📋 19. КРАТКАЯ СВОДКА")
    lines.append(f"   ▫️ Состояние: {'🟢 Жив' if status != 'critical' else '🔴 Почти не отвечает'}")
    
    if avg_time is not None:
        if avg_time < 300:
            lines.append(f"   ▫️ Скорость: 🚀 Отличная")
        elif avg_time < 600:
            lines.append(f"   ▫️ Скорость: 👍 Нормальная")
        else:
            lines.append(f"   ▫️ Скорость: 🐢 Медленная")
    else:
        lines.append(f"   ▫️ Скорость: ❓ Нет данных")
    
    lines.append(f"   ▫️ Стабильность: {'🟢 Высокая' if deep.get('stability_ok',0) >= 2 else '🟡 Средняя' if deep.get('stability_ok',0) >= 1 else '🔴 Низкая'}")
    lines.append(f"   ▫️ Надёжность: {'✅ Хорошая' if uptime >= 95 else '⚠️ Средняя' if uptime >= 85 else '❌ Плохая'}")
    lines.append("")
    
    total_checks = stats.get('count', 0)
    lines.append(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')} | 📊 Всего проверок: {total_checks}")
    
    return "\n".join(lines)


# ========== INLINE КЛАВИАТУРЫ ==========

def get_main_keyboard(bot_username: str):
    buttons = [
        [InlineKeyboardButton(text="🔍 Быстрая проверка", callback_data=f"check_{bot_username}")],
        [InlineKeyboardButton(text="📄 Полный отчёт", callback_data=f"fullreport_{bot_username}")],
        [InlineKeyboardButton(text="➕ Добавить в мониторинг", callback_data=f"add_{bot_username}")],
        [InlineKeyboardButton(text="➖ Удалить из мониторинга", callback_data=f"remove_{bot_username}")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data=f"stats_{bot_username}")],
        [InlineKeyboardButton(text="🔄 Другой бот", callback_data="clear")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ========== HTTP СЕРВЕР ДЛЯ RENDER ==========
async def health_check(request):
    return web.Response(text="OK")

async def start_http_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080)))
    await site.start()
    logger.info(f"✅ HTTP сервер запущен на порту {os.environ.get('PORT', 8080)}")
    await asyncio.Event().wait()


# ========== ОБРАБОТЧИКИ ==========

@dp.message()
async def handle_message(msg: Message):
    text = msg.text.strip()
    
    if text == '/start':
        await msg.reply(
            "🤖 **Монитор ботов**\n\n"
            "📌 Просто отправь мне @username бота\n\n"
            "⬇️ Напиши @username бота для начала"
        )
        return
    
    if text.startswith('@'):
        username = text[1:]
        await msg.reply(
            f"🤖 Выбран бот @{username}\n\nВыберите действие:",
            reply_markup=get_main_keyboard(username)
        )
    else:
        await msg.reply(
            "❌ Отправь @username бота.\n\n"
            "Пример: @example_bot"
        )


@dp.callback_query()
async def handle_callback(callback: CallbackQuery):
    data = callback.data
    
    if data == "clear":
        await callback.message.edit_text(
            "🔄 Отправь @username другого бота для проверки"
        )
        await callback.answer()
        return
    
    parts = data.split("_", 1)
    if len(parts) != 2:
        await callback.answer("Неизвестная команда")
        return
    
    action, username = parts
    
    if action == "check":
        await callback.answer("🔍 Выполняю быструю проверку...")
        report = await generate_quick_report(username)
        await callback.message.edit_text(report, reply_markup=get_main_keyboard(username))
    
    elif action == "fullreport":
        await callback.answer("📄 Генерирую полный отчёт (20-30 сек)...")
        report = await generate_full_report(username)
        await callback.message.edit_text(report, reply_markup=get_main_keyboard(username))
    
    elif action == "add":
        watched_bots.add(username)
        await callback.answer(f"✅ @{username} добавлен в мониторинг")
        await callback.message.edit_text(
            f"✅ @{username} добавлен в мониторинг\nПроверка раз в час\n\nВыберите действие:",
            reply_markup=get_main_keyboard(username)
        )
    
    elif action == "remove":
        watched_bots.discard(username)
        await callback.answer(f"✅ @{username} удалён из мониторинга")
        await callback.message.edit_text(
            f"✅ @{username} удалён из мониторинга\n\nВыберите действие:",
            reply_markup=get_main_keyboard(username)
        )
    
    elif action == "stats":
        await callback.answer("📊 Загружаю статистику...")
        stats_data = get_stats(username, 20)
        if stats_data['count'] == 0:
            await callback.message.edit_text(f"📊 Нет данных по @{username}\n\nСначала выполните быструю проверку", reply_markup=get_main_keyboard(username))
        else:
            deep = get_latest_deep(username)
            score = deep.get('score', 0) if deep else 0
            status = get_status(username)
            status_emoji = "🟢" if status == "stable" else "🟡" if status == "warning" else "🔴"
            
            report = f"📊 **Статистика** @{username}\n"
            report += f"{status_emoji} Статус: {status.upper()}\n"
            report += f"⚡ Средняя скорость: {stats_data['avg_time']:.0f} мс\n"
            report += f"🏆 Бот скор: {score}/100\n"
            report += f"📈 Всего замеров: {stats_data['count']}"
            await callback.message.edit_text(report, reply_markup=get_main_keyboard(username))
    
    else:
        await callback.answer("Неизвестная команда")
    
    await callback.answer()


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
    
    asyncio.create_task(start_http_server())
    
    await bot_client.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(1)
    
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
