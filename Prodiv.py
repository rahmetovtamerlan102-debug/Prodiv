#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import time
import signal
import logging
import statistics
import tempfile
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, Set

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, types, Router
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command

from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession

import aiosqlite
from aiohttp import web

# ======================
# КОНФИГУРАЦИЯ
# ======================

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

DB_PATH = "checks.db"
PING_TIMEOUT = 10
RESPONSE_TIMEOUT = 8
MONITOR_INTERVAL = 60      # секунд между фоновыми проверками
WEB_PORT = int(os.getenv("PORT", "8080"))   # порт для health check

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ======================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ======================

bot: Bot = None
client: TelegramClient = None
db: aiosqlite.Connection = None
shutdown_event = asyncio.Event()
shutdown_in_progress = False
router = Router()
pending_checks: Dict[int, asyncio.Queue] = {}

watched_cache: Set[str] = set()
watched_cache_lock = asyncio.Lock()

# ======================
# БАЗА ДАННЫХ
# ======================

async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("""
    CREATE TABLE IF NOT EXISTS checks (
        id INTEGER PRIMARY KEY,
        bot TEXT,
        bot_id INTEGER,
        ts INTEGER,
        success INTEGER,
        rt INTEGER,
        error TEXT
    )
    """)
    await db.execute("""
    CREATE TABLE IF NOT EXISTS watched (
        bot TEXT PRIMARY KEY,
        added_at INTEGER
    )
    """)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_checks_bot ON checks(bot)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_checks_ts ON checks(ts)")
    await db.commit()
    logger.info("База данных готова")

async def save_check(botname: str, bot_id: int, success: bool, rt: Optional[int], error: str = ""):
    now = int(time.time())
    await db.execute(
        "INSERT INTO checks(bot, bot_id, ts, success, rt, error) VALUES (?,?,?,?,?,?)",
        (botname, bot_id, now, 1 if success else 0, rt, error)
    )
    await db.commit()

# ======================
# УПРАВЛЕНИЕ СПИСКОМ НАБЛЮДАЕМЫХ
# ======================

async def refresh_watched_cache():
    global watched_cache
    async with db.execute("SELECT bot FROM watched") as cursor:
        rows = await cursor.fetchall()
    async with watched_cache_lock:
        watched_cache = {row[0] for row in rows}

async def add_watched(botname: str):
    async with db.execute(
        "INSERT OR IGNORE INTO watched (bot, added_at) VALUES (?, ?)",
        (botname, int(time.time()))
    ):
        await db.commit()
    await refresh_watched_cache()

async def remove_watched(botname: str):
    async with db.execute("DELETE FROM watched WHERE bot = ?", (botname,)):
        await db.commit()
    await refresh_watched_cache()

async def is_watched(botname: str) -> bool:
    async with watched_cache_lock:
        return botname in watched_cache

async def get_watched_list() -> list:
    async with watched_cache_lock:
        return sorted(list(watched_cache))

# ======================
# ФОНОВЫЙ МОНИТОРИНГ (LIVE)
# ======================

async def monitor_single_bot(botname: str):
    while not shutdown_event.is_set():
        try:
            ok, _, _, err = await check_bot(botname)
            if not ok:
                logger.warning(f"LIVE: {botname} не ответил: {err}")
            else:
                logger.info(f"LIVE: {botname} проверен")
        except Exception as e:
            logger.error(f"LIVE ошибка {botname}: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)

async def background_monitor():
    monitor_tasks = {}
    while not shutdown_event.is_set():
        current_watched = set()
        async with watched_cache_lock:
            current_watched = watched_cache.copy()
        for botname in current_watched:
            if botname not in monitor_tasks or monitor_tasks[botname].done():
                monitor_tasks[botname] = asyncio.create_task(monitor_single_bot(botname))
                logger.info(f"Запущен фоновый мониторинг @{botname}")
        for botname in list(monitor_tasks.keys()):
            if botname not in current_watched:
                monitor_tasks[botname].cancel()
                del monitor_tasks[botname]
                logger.info(f"Остановлен мониторинг @{botname}")
        await asyncio.sleep(5)

# ======================
# ГЛОБАЛЬНЫЙ ОБРАБОТЧИК СОБЫТИЙ (ДЛЯ ОТВЕТОВ БОТОВ)
# ======================

async def setup_global_handlers():
    @client.on(events.NewMessage)
    async def message_handler(event):
        sender_id = event.sender_id
        if sender_id and sender_id in pending_checks:
            await pending_checks[sender_id].put(("message", event))
    @client.on(events.MessageEdited)
    async def edit_handler(event):
        sender_id = event.sender_id
        if sender_id and sender_id in pending_checks:
            await pending_checks[sender_id].put(("edit", event))
    @client.on(events.CallbackQuery)
    async def callback_handler(event):
        user_id = event.user_id
        if user_id and user_id in pending_checks:
            await pending_checks[user_id].put(("callback", event))
    logger.info("Глобальные обработчики установлены")

# ======================
# ПРОВЕРКА БОТА (ОТПРАВКА /start, ОЖИДАНИЕ ЛЮБОГО ОТВЕТА)
# ======================

async def ensure_telethon() -> bool:
    global client
    if not client: return False
    try:
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=10)
        if not await client.is_user_authorized():
            logger.error("Telethon не авторизован")
            return False
        return True
    except Exception as e:
        logger.error(f"Telethon ошибка: {e}")
        return False

async def check_bot(username: str) -> Tuple[bool, int, int, str]:
    if not await ensure_telethon():
        return False, 0, 0, "Telethon не готов"
    try:
        entity = await asyncio.wait_for(client.get_entity(username), timeout=PING_TIMEOUT)
        if not getattr(entity, 'bot', False):
            return False, 0, 0, "Не является ботом"
        bot_id = entity.id
        queue = asyncio.Queue()
        pending_checks[bot_id] = queue
        start = time.perf_counter()
        await client.send_message(entity, "/start")
        try:
            _, _ = await asyncio.wait_for(queue.get(), timeout=RESPONSE_TIMEOUT)
            rt = int((time.perf_counter() - start) * 1000)
            await save_check(username, bot_id, True, rt, "")
            return True, bot_id, rt, ""
        except asyncio.TimeoutError:
            await save_check(username, bot_id, False, None, "Бот не ответил")
            return False, bot_id, 0, "Нет ответа"
        finally:
            pending_checks.pop(bot_id, None)
    except asyncio.TimeoutError:
        return False, 0, 0, "Таймаут получения бота"
    except errors.FloodWaitError as e:
        await asyncio.sleep(min(e.seconds, 30))
        return False, 0, 0, f"Flood на {e.seconds}с"
    except errors.rpcerrorlist.UsernameNotOccupiedError:
        return False, 0, 0, "Имя не существует"
    except Exception as e:
        return False, 0, 0, str(e)[:60]

# ======================
# ВСПОМОГАТЕЛЬНЫЕ СТАТИСТИЧЕСКИЕ ФУНКЦИИ (для отчёта)
# ======================

async def get_bot_id_from_db(botname: str) -> int:
    async with db.execute("SELECT bot_id FROM checks WHERE bot=? AND bot_id>0 ORDER BY ts DESC LIMIT 1", (botname,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_first_seen(botname: str) -> str:
    async with db.execute("SELECT MIN(ts) FROM checks WHERE bot=?", (botname,)) as cursor:
        row = await cursor.fetchone()
    if row and row[0]:
        return datetime.fromtimestamp(row[0]).strftime("%d.%m.%Y %H:%M:%S")
    return "Нет данных"

async def get_last_seen(botname: str) -> str:
    async with db.execute("SELECT MAX(ts) FROM checks WHERE bot=?", (botname,)) as cursor:
        row = await cursor.fetchone()
    if row and row[0]:
        return datetime.fromtimestamp(row[0]).strftime("%d.%m.%Y %H:%M:%S")
    return "Нет данных"

async def get_total_checks(botname: str) -> int:
    async with db.execute("SELECT COUNT(*) FROM checks WHERE bot=?", (botname,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_success_count(botname: str) -> int:
    async with db.execute("SELECT COUNT(*) FROM checks WHERE bot=? AND success=1", (botname,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_fail_count(botname: str) -> int:
    async with db.execute("SELECT COUNT(*) FROM checks WHERE bot=? AND success=0", (botname,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_success_rate(botname: str) -> float:
    total = await get_total_checks(botname)
    success = await get_success_count(botname)
    return (success / total * 100) if total else 0

async def get_avg_response_time(botname: str) -> int:
    async with db.execute("SELECT AVG(rt) FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0", (botname,)) as cursor:
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] else 0

async def get_min_response_time(botname: str) -> int:
    async with db.execute("SELECT MIN(rt) FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0", (botname,)) as cursor:
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] else 0

async def get_max_response_time(botname: str) -> int:
    async with db.execute("SELECT MAX(rt) FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0", (botname,)) as cursor:
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] else 0

async def get_last_response_time(botname: str) -> int:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0 ORDER BY ts DESC LIMIT 1", (botname,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_median_response_time(botname: str) -> int:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY rt", (botname,)) as cursor:
        rows = await cursor.fetchall()
    if not rows: return 0
    rts = [r[0] for r in rows]
    mid = len(rts)//2
    return int((rts[mid-1]+rts[mid])/2) if len(rts)%2==0 else int(rts[mid])

async def get_p95_response_time(botname: str) -> int:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY rt", (botname,)) as cursor:
        rows = await cursor.fetchall()
    if not rows: return 0
    rts = [r[0] for r in rows]
    idx = int(len(rts)*0.95)
    return rts[idx] if idx < len(rts) else rts[-1]

async def get_response_time_distribution(botname: str) -> Dict:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0", (botname,)) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts:
        return {"fast":0,"normal":0,"slow":0,"very_slow":0}
    total = len(rts)
    fast = sum(1 for r in rts if r<200)/total*100
    norm = sum(1 for r in rts if 200<=r<500)/total*100
    slow = sum(1 for r in rts if 500<=r<1000)/total*100
    vslow = sum(1 for r in rts if r>=1000)/total*100
    return {"fast":round(fast),"normal":round(norm),"slow":round(slow),"very_slow":round(vslow)}

async def get_jitter(botname: str) -> int:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 30", (botname,)) as cursor:
        rows = await cursor.fetchall()
    if len(rows)<3: return 0
    rts = [r[0] for r in rows]
    return int(statistics.stdev(rts)) if len(rts)>1 else 0

async def get_trend_direction(botname: str) -> str:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 20", (botname,)) as cursor:
        rows = await cursor.fetchall()
    if len(rows)<6: return "Недостаточно данных"
    rts = [r[0] for r in rows]
    mid = len(rts)//2
    old_avg = statistics.mean(rts[mid:])
    new_avg = statistics.mean(rts[:mid])
    if old_avg==0: return "Стабильно"
    change = ((new_avg-old_avg)/old_avg)*100
    if change>15: return "Ухудшается"
    if change<-15: return "Улучшается"
    return "Стабильно"

async def get_trend_change(botname: str) -> int:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 20", (botname,)) as cursor:
        rows = await cursor.fetchall()
    if len(rows)<6: return 0
    rts = [r[0] for r in rows]
    mid = len(rts)//2
    old_avg = statistics.mean(rts[mid:])
    new_avg = statistics.mean(rts[:mid])
    if old_avg==0: return 0
    return abs(round(((new_avg-old_avg)/old_avg)*100))

async def get_uptime_last_hour(botname: str) -> float:
    since = int(time.time())-3600
    async with db.execute("SELECT success FROM checks WHERE bot=? AND ts>?", (botname, since)) as cursor:
        rows = await cursor.fetchall()
    if not rows: return 0
    return sum(1 for r in rows if r[0]==1)/len(rows)*100

async def get_uptime_last_day(botname: str) -> float:
    since = int(time.time())-86400
    async with db.execute("SELECT success FROM checks WHERE bot=? AND ts>?", (botname, since)) as cursor:
        rows = await cursor.fetchall()
    if not rows: return 0
    return sum(1 for r in rows if r[0]==1)/len(rows)*100

async def get_uptime_last_week(botname: str) -> float:
    since = int(time.time())-604800
    async with db.execute("SELECT success FROM checks WHERE bot=? AND ts>?", (botname, since)) as cursor:
        rows = await cursor.fetchall()
    if not rows: return 0
    return sum(1 for r in rows if r[0]==1)/len(rows)*100

async def get_health_score(botname: str) -> int:
    success_rate = await get_success_rate(botname)
    avg_rt = await get_avg_response_time(botname)
    score = 100
    if avg_rt > 1000: score -= 25
    elif avg_rt > 500: score -= 15
    elif avg_rt > 300: score -= 8
    elif avg_rt > 200: score -= 3
    if success_rate < 70: score -= 30
    elif success_rate < 85: score -= 15
    elif success_rate < 95: score -= 5
    return max(0, min(100, score))

async def get_reliability_score(botname: str) -> int:
    success_rate = await get_success_rate(botname)
    avg_rt = await get_avg_response_time(botname)
    rel = success_rate * 0.6 + max(0, 100 - avg_rt/10) * 0.4
    return min(100, max(0, int(rel)))

async def get_risk_level(botname: str) -> str:
    success_rate = await get_success_rate(botname)
    avg_rt = await get_avg_response_time(botname)
    fail_count = await get_fail_count(botname)
    risk = 0
    if success_rate < 80: risk+=30
    elif success_rate < 90: risk+=15
    if avg_rt > 500: risk+=20
    elif avg_rt > 300: risk+=10
    if fail_count > 10: risk+=20
    elif fail_count > 5: risk+=10
    if risk<20: return "Низкий"
    if risk<50: return "Средний"
    return "Высокий"

async def get_prediction(botname: str) -> str:
    trend = await get_trend_direction(botname)
    success_rate = await get_success_rate(botname)
    if trend == "Улучшается" and success_rate>90: return "Рост"
    if trend == "Ухудшается" or success_rate<80: return "Спад"
    return "Стабильность"

async def get_sla_grade(botname: str) -> str:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0", (botname,)) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts: return "Н/Д"
    sla500 = sum(1 for r in rts if r<500)/len(rts)*100
    if sla500 >= 99: return "A+"
    if sla500 >= 95: return "A"
    if sla500 >= 90: return "B"
    if sla500 >= 80: return "C"
    return "D"

async def get_sla_500(botname: str) -> float:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0", (botname,)) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts: return 0
    return sum(1 for r in rts if r<500)/len(rts)*100

async def get_sla_1000(botname: str) -> float:
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0", (botname,)) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts: return 0
    return sum(1 for r in rts if r<1000)/len(rts)*100

async def get_bot_classification(botname: str) -> str:
    avg_rt = await get_avg_response_time(botname)
    jitter = await get_jitter(botname)
    if avg_rt < 200 and jitter < 50: return "Высокопроизводительный (Элитный)"
    if avg_rt < 400: return "Стандартный (Обычный)"
    return "Медленный (Базовый)"

async def get_last_check_status(botname: str) -> str:
    async with db.execute("SELECT success, error FROM checks WHERE bot=? ORDER BY ts DESC LIMIT 1", (botname,)) as cursor:
        row = await cursor.fetchone()
    if not row: return "Нет проверок"
    if row[0]==1: return "Успешно"
    return f"Ошибка: {row[1] if row[1] else 'Неизвестно'}"

async def get_database_record_count(botname: str) -> int:
    async with db.execute("SELECT COUNT(*) FROM checks WHERE bot=?", (botname,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_data_age_days(botname: str) -> int:
    first = await get_first_seen(botname)
    if first == "Нет данных": return 0
    try:
        first_date = datetime.strptime(first, "%d.%m.%Y %H:%M:%S")
        return (datetime.now() - first_date).days
    except:
        return 0

async def get_data_confidence(botname: str) -> int:
    total = await get_total_checks(botname)
    return min(100, total*5)

# ======================
# 40 СЕКЦИЙ ОТЧЁТА
# ======================

async def generate_full_report(botname: str) -> str:
    bot_id = await get_bot_id_from_db(botname)
    first_seen = await get_first_seen(botname)
    last_seen = await get_last_seen(botname)
    total_checks = await get_total_checks(botname)
    success_count = await get_success_count(botname)
    fail_count = await get_fail_count(botname)
    success_rate = await get_success_rate(botname)
    avg_rt = await get_avg_response_time(botname)
    min_rt = await get_min_response_time(botname)
    max_rt = await get_max_response_time(botname)
    median_rt = await get_median_response_time(botname)
    p95_rt = await get_p95_response_time(botname)
    last_rt = await get_last_response_time(botname)
    dist = await get_response_time_distribution(botname)
    jitter = await get_jitter(botname)
    trend_dir = await get_trend_direction(botname)
    trend_change = await get_trend_change(botname)
    uptime_hour = await get_uptime_last_hour(botname)
    uptime_day = await get_uptime_last_day(botname)
    uptime_week = await get_uptime_last_week(botname)
    health = await get_health_score(botname)
    reliability = await get_reliability_score(botname)
    risk = await get_risk_level(botname)
    prediction = await get_prediction(botname)
    sla_grade = await get_sla_grade(botname)
    sla_500 = await get_sla_500(botname)
    sla_1000 = await get_sla_1000(botname)
    classification = await get_bot_classification(botname)
    last_status = await get_last_check_status(botname)
    record_count = await get_database_record_count(botname)
    data_age_days = await get_data_age_days(botname)
    confidence = await get_data_confidence(botname)

    if success_rate > 95:
        status_text = "Онлайн 🟢"
        status_desc = "Бот работает стабильно и отвечает на запросы"
    elif success_rate > 80:
        status_text = "Нестабилен 🟡"
        status_desc = "Бот работает с перебоями, возможны сбои"
    else:
        status_text = "Офлайн 🔴"
        status_desc = "Бот не отвечает на запросы, требуется проверка"

    if health >= 85:
        verdict = "Отличный бот"
        verdict_desc = "Бот показывает высокую производительность и стабильность"
    elif health >= 70:
        verdict = "Хороший бот"
        verdict_desc = "Бот работает хорошо, но есть небольшие проблемы"
    elif health >= 50:
        verdict = "Средний бот"
        verdict_desc = "Бот работает со средними показателями, требует внимания"
    else:
        verdict = "Плохой бот"
        verdict_desc = "Бот работает нестабильно, не рекомендуется к использованию"

    lines = []
    lines.append(f"🤖 ОТЧЁТ ПО БОТУ @{botname}")
    lines.append("")

    # 1
    lines.append("1. ИНФОРМАЦИЯ О БОТЕ")
    lines.append(f"   Имя пользователя: @{botname}")
    lines.append(f"   Уникальный ID: {bot_id if bot_id else 'не определён'}")
    lines.append(f"   Тип аккаунта: Telegram Бот")
    lines.append(f"   Статус: {status_text}")
    lines.append(f"   Общее состояние: {verdict_desc}")
    lines.append("")

    # 2
    lines.append("2. ИСТОРИЯ ПРОВЕРОК")
    lines.append(f"   Всего проведено проверок: {total_checks}")
    lines.append(f"   Из них успешных: {success_count}")
    lines.append(f"   Из них неудачных: {fail_count}")
    lines.append(f"   Общая успешность: {success_rate:.1f}%")
    lines.append(f"   Последний статус: {last_status}")
    lines.append("")

    # 3
    lines.append("3. ВРЕМЕННЫЕ МЕТРИКИ")
    lines.append(f"   Первое появление в системе: {first_seen}")
    lines.append(f"   Последнее появление в системе: {last_seen}")
    lines.append(f"   Возраст данных: {data_age_days} дней")
    lines.append(f"   Количество записей в БД: {record_count}")
    lines.append(f"   Достоверность данных: {confidence}%")
    lines.append("")

    # 4
    lines.append("4. СТАТИСТИКА ВРЕМЕНИ ОТВЕТА")
    lines.append(f"   Среднее время: {avg_rt} мс")
    lines.append(f"   Медианное время: {median_rt} мс")
    lines.append(f"   Минимальное время: {min_rt} мс")
    lines.append(f"   Максимальное время: {max_rt} мс")
    lines.append(f"   P95 (95% ответов быстрее): {p95_rt} мс")
    lines.append("")

    # 5
    lines.append("5. ПОСЛЕДНЯЯ ПРОВЕРКА")
    lines.append(f"   Время ответа при последней проверке: {last_rt} мс")
    lines.append(f"   Статус последней проверки: {last_status}")
    lines.append(f"   Время последней проверки: {last_seen}")
    lines.append(f"   Общая успешность за всё время: {success_rate:.1f}%")
    lines.append(f"   Тренд изменения: {trend_dir}")
    lines.append("")

    # 6
    lines.append("6. РАСПРЕДЕЛЕНИЕ СКОРОСТИ ОТВЕТОВ")
    lines.append(f"   Быстрые ответы (менее 200мс): {dist['fast']}%")
    lines.append(f"   Нормальные ответы (200-500мс): {dist['normal']}%")
    lines.append(f"   Медленные ответы (500-1000мс): {dist['slow']}%")
    lines.append(f"   Очень медленные ответы (более 1000мс): {dist['very_slow']}%")
    lines.append("")

    # 7
    lines.append("7. СТАБИЛЬНОСТЬ РАБОТЫ")
    lines.append(f"   Джиттер (отклонение времени ответа): {jitter} мс")
    if jitter < 50:
        lines.append("   Оценка стабильности: Отличная")
        lines.append("   Время ответа стабильное, без резких скачков")
    elif jitter < 100:
        lines.append("   Оценка стабильности: Хорошая")
        lines.append("   Небольшие колебания времени ответа")
    elif jitter < 200:
        lines.append("   Оценка стабильности: Средняя")
        lines.append("   Заметны колебания времени ответа")
    else:
        lines.append("   Оценка стабильности: Плохая")
        lines.append("   Время ответа сильно скачет, нестабильная работа")
    lines.append("")

    # 8
    lines.append("8. ТРЕНД ИЗМЕНЕНИЙ")
    lines.append(f"   Направление: {trend_dir}")
    lines.append(f"   Изменение: {trend_change}%")
    if trend_dir == "Улучшается":
        lines.append("   Бот показывает положительную динамику")
        lines.append("   Время ответа уменьшается")
    elif trend_dir == "Ухудшается":
        lines.append("   Бот показывает отрицательную динамику")
        lines.append("   Время ответа увеличивается, требуется внимание")
    else:
        lines.append("   Бот стабилен, без резких изменений")
        lines.append("   Текущее состояние не вызывает опасений")
    lines.append("")

    # 9
    lines.append("9. ВРЕМЯ РАБОТЫ (UPTIME)")
    lines.append(f"   За последний час: {uptime_hour:.1f}%")
    lines.append(f"   За последние 24 часа: {uptime_day:.1f}%")
    lines.append(f"   За последние 7 дней: {uptime_week:.1f}%")
    if uptime_day > 95:
        lines.append("   Доступность: Отличная")
        lines.append("   Бот почти всегда доступен")
    elif uptime_day > 80:
        lines.append("   Доступность: Средняя")
        lines.append("   Бот иногда недоступен")
    else:
        lines.append("   Доступность: Низкая")
        lines.append("   Бот часто недоступен, проверьте его состояние")
    lines.append("")

    # 10
    lines.append("10. ОЦЕНКА ЗДОРОВЬЯ БОТА")
    lines.append(f"   Общая оценка: {health}/100")
    lines.append(f"   Уровень: {verdict}")
    lines.append(f"   Надёжность: {reliability}/100")
    lines.append("")

    # 11
    lines.append("11. АНАЛИЗ РИСКОВ")
    lines.append(f"   Уровень риска: {risk}")
    if risk == "Низкий":
        lines.append("   Описание: Риски минимальны, бот можно использовать без опасений")
    elif risk == "Средний":
        lines.append("   Описание: Присутствуют риски сбоев, рекомендуется мониторинг")
    else:
        lines.append("   Описание: Высокие риски, бот часто недоступен или медленно отвечает")
    lines.append("")

    # 12
    lines.append("12. AI ПРОГНОЗ")
    lines.append(f"   Прогноз на ближайшее время: {prediction}")
    if prediction == "Рост":
        lines.append("   Ожидается улучшение производительности")
    elif prediction == "Спад":
        lines.append("   Ожидается ухудшение производительности")
    else:
        lines.append("   Ожидается стабильная работа")
    lines.append("")

    # 13
    lines.append("13. УРОВЕНЬ ОБСЛУЖИВАНИЯ (SLA)")
    lines.append(f"   Доля ответов до 500мс: {sla_500:.1f}%")
    lines.append(f"   Доля ответов до 1000мс: {sla_1000:.1f}%")
    lines.append(f"   Итоговая оценка: {sla_grade}")
    lines.append("")

    # 14
    lines.append("14. КЛАССИФИКАЦИЯ БОТА")
    lines.append(f"   Тип: {classification}")
    lines.append("")

    # 15
    lines.append("15. РЕКОМЕНДАЦИИ ПО УЛУЧШЕНИЮ")
    if avg_rt > 500:
        lines.append("   • Оптимизируйте время ответа сервера")
    if success_rate < 90:
        lines.append("   • Проверьте доступность и стабильность бота")
    if jitter > 100:
        lines.append("   • Уменьшите джиттер, стабилизируйте соединение")
    if total_checks < 20:
        lines.append("   • Проведите больше проверок для точной статистики")
    if uptime_day < 95:
        lines.append("   • Улучшите аптайм, настройте мониторинг")
    if avg_rt <= 500 and success_rate >= 90 and jitter <= 100 and total_checks >= 20:
        lines.append("   • Все метрики в норме, бот работает отлично")
    lines.append("")

    # 16
    lines.append("16. ИТОГОВАЯ ОЦЕНКА")
    lines.append(f"   Вердикт: {verdict}")
    lines.append(f"   Рекомендуется к использованию: {'Да' if health >= 70 else 'С осторожностью' if health >= 50 else 'Нет'}")
    lines.append("")

    # 17
    lines.append("17. МЕТОД ПРОВЕРКИ")
    lines.append("   Отправка /start целевому боту")
    lines.append("   Ожидание ответа (текст, кнопки, редактирование)")
    lines.append(f"   Таймаут ожидания: {RESPONSE_TIMEOUT} секунд")
    lines.append("")

    # 18
    lines.append("18. ИСТОРИЯ ПРОВЕРОК (ДЕТАЛИ)")
    lines.append(f"   Первая проверка: {first_seen}")
    lines.append(f"   Последняя проверка: {last_seen}")
    lines.append(f"   Средний интервал: {'более 24ч' if total_checks<2 else f'{(data_age_days*86400)/total_checks:.0f} сек'}")
    lines.append("")

    # 19
    lines.append("19. ПАРАМЕТРЫ СЕТИ")
    lines.append(f"   Джиттер (вариативность): {jitter} мс")
    lines.append(f"   Среднее отклонение: {jitter*0.7:.1f} мс")
    lines.append("")

    # 20
    lines.append("20. АКТИВНОСТЬ БОТА")
    lines.append(f"   Успешность ответов: {success_rate:.1f}%")
    lines.append(f"   Частота проверок: {total_checks} раз")
    lines.append("")

    # 21
    lines.append("21. НАГРУЗКА НА БОТА")
    lines.append(f"   Среднее время обработки: {avg_rt} мс")
    lines.append(f"   Пиковая нагрузка (max): {max_rt} мс")
    lines.append("")

    # 22
    lines.append("22. РАБОТОСПОСОБНОСТЬ")
    lines.append(f"   Uptime за сутки: {uptime_day:.1f}%")
    lines.append(f"   Uptime за неделю: {uptime_week:.1f}%")
    lines.append("")

    # 23
    lines.append("23. СРАВНЕНИЕ С НОРМОЙ")
    lines.append(f"   Отклонение от среднего: {'ниже' if avg_rt<300 else 'выше'}")
    lines.append(f"   Стабильность: {'хорошая' if jitter<100 else 'плохая'}")
    lines.append("")

    # 24
    lines.append("24. КАЧЕСТВО ОБСЛУЖИВАНИЯ")
    lines.append(f"   SLA 500ms: {sla_500:.1f}%")
    lines.append(f"   SLA 1000ms: {sla_1000:.1f}%")
    lines.append("")

    # 25
    lines.append("25. ПРОГНОЗ НАГРУЗКИ")
    lines.append(f"   Тренд: {trend_dir}")
    lines.append(f"   Ожидаемое время через неделю: {int(avg_rt * (1 + trend_change/100)) if trend_change else avg_rt} мс")
    lines.append("")

    # 26
    lines.append("26. РИСКИ")
    lines.append(f"   Вероятность сбоя: {100 - success_rate:.1f}%")
    lines.append(f"   Рекомендация: {'Наблюдать' if risk != 'Низкий' else 'Спокойно использовать'}")
    lines.append("")

    # 27
    lines.append("27. ЭФФЕКТИВНОСТЬ")
    lines.append(f"   Индекс надёжности: {reliability}/100")
    lines.append(f"   Индекс производительности: {100 - avg_rt//10 if avg_rt<1000 else 0}/100")
    lines.append("")

    # 28
    lines.append("28. КЛАСТЕРИЗАЦИЯ")
    lines.append(f"   Группа: {classification.split(' ')[0]}")
    lines.append(f"   Конкурентоспособность: {'Высокая' if health>80 else 'Средняя' if health>60 else 'Низкая'}")
    lines.append("")

    # 29
    lines.append("29. ДОЛГОВЕЧНОСТЬ")
    lines.append(f"   Данных накоплено: {data_age_days} дней")
    lines.append(f"   Стабильность прогноза: {'Высокая' if confidence>70 else 'Средняя'}")
    lines.append("")

    # 30
    lines.append("30. ИТОГОВАЯ НАДЁЖНОСТЬ")
    lines.append(f"   Общая надёжность: {reliability}/100")
    lines.append(f"   Риск отказа: {risk}")
    lines.append("")

    # 31
    lines.append("31. ВРЕМЯ ВОССТАНОВЛЕНИЯ")
    lines.append(f"   Среднее время простоя: {100 - uptime_day:.1f}% времени")
    lines.append(f"   Приоритет исправления: {'Высокий' if uptime_day<90 else 'Средний' if uptime_day<95 else 'Низкий'}")
    lines.append("")

    # 32
    lines.append("32. ЗАПАС ПРОЧНОСТИ")
    lines.append(f"   Буфер времени: {max(0, 500 - avg_rt)} мс до порога 500мс")
    lines.append("")

    # 33
    lines.append("33. АДАПТИВНОСТЬ")
    lines.append(f"   Скорость реакции на изменения: {'Быстрая' if trend_change<5 else 'Медленная'}")
    lines.append("")

    # 34
    lines.append("34. ПОТРЕБЛЕНИЕ РЕСУРСОВ")
    lines.append(f"   Потенциальная нагрузка на сервер: {'Высокая' if avg_rt>500 else 'Нормальная'}")
    lines.append("")

    # 35
    lines.append("35. МАСШТАБИРУЕМОСТЬ")
    lines.append(f"   Ожидаемая производительность: {classification}")
    lines.append("")

    # 36
    lines.append("36. ДОВЕРИТЕЛЬНЫЙ ИНТЕРВАЛ")
    lines.append(f"   Точность данных: {confidence}%")
    lines.append("")

    # 37
    lines.append("37. СТАТИСТИЧЕСКАЯ ЗНАЧИМОСТЬ")
    lines.append(f"   Выборка: {total_checks} измерений")
    lines.append(f"   Достоверность тренда: {'Высокая' if total_checks>10 else 'Низкая'}")
    lines.append("")

    # 38
    lines.append("38. ОПТИМИЗАЦИЯ")
    lines.append(f"   Рекомендуемый лимит времени: {max(200, avg_rt)} мс")
    lines.append("")

    # 39
    lines.append("39. ДОПОЛНИТЕЛЬНЫЕ МЕТРИКИ")
    lines.append(f"   Медиана/Среднее: {median_rt}/{avg_rt} мс")
    lines.append(f"   Разброс значений: {max_rt - min_rt} мс")
    lines.append("")

    # 40
    lines.append("40. ЗАКЛЮЧЕНИЕ")
    lines.append(f"   Бот @{botname} имеет оценку здоровья {health}/100.")
    lines.append(f"   Статус: {status_text}.")
    lines.append(f"   Рекомендация: {verdict}.")
    lines.append("")

    return "\n".join(lines)

# ======================
# ВЕБ-СЕРВЕР ДЛЯ HEALTH CHECK (LIVE)
# ======================

async def health_check(request):
    return web.Response(text="OK", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    logger.info(f"Health check сервер запущен на порту {WEB_PORT}")

# ======================
# КОМАНДЫ БОТА
# ======================

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "🤖 **Анализатор ботов (Live режим)**\n\n"
        "📌 **Команды:**\n"
        "▪️ /check @bot — быстрая проверка\n"
        "▪️ /report @bot — полный отчёт (40 секций, файл)\n"
        "▪️ /stats @bot — статистика\n"
        "▪️ /add @bot — добавить в фоновый мониторинг (проверка раз в минуту)\n"
        "▪️ /remove @bot — удалить из мониторинга\n"
        "▪️ /list — список отслеживаемых ботов\n\n"
        "Пример: /report @example_bot"
    )

@router.message(Command("check"))
async def cmd_check(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /check @username")
        return
    botname = args[1].lstrip("@")
    await m.answer(f"⏳ Проверка @{botname}...")
    ok, bot_id, rt, err = await check_bot(botname)
    if ok:
        await m.answer(f"✅ @{botname}\n⏱️ Ответ за {rt} мс\n🆔 ID: {bot_id}")
    else:
        await m.answer(f"❌ @{botname}\n📛 Ошибка: {err}")

@router.message(Command("report"))
async def cmd_report(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /report @username")
        return
    botname = args[1].lstrip("@")
    await m.answer(f"⏳ Генерация отчёта для @{botname}...")
    ok, _, _, err = await check_bot(botname)
    if not ok:
        await m.answer(f"⚠️ Бот не ответил: {err}")
    report_text = await generate_full_report(botname)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(report_text)
        temp_path = f.name
    await m.answer_document(
        document=FSInputFile(temp_path, filename=f"report_{botname}_{int(time.time())}.txt"),
        caption=f"📋 Отчёт по боту @{botname} (40 секций)"
    )
    os.unlink(temp_path)

@router.message(Command("stats"))
async def cmd_stats(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /stats @username")
        return
    botname = args[1].lstrip("@")
    async with db.execute(
        "SELECT success, rt FROM checks WHERE bot=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT 30",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        await m.answer(f"📊 Нет данных для @{botname}\nСделайте /check @{botname}")
        return
    rts = [r[1] for r in rows if r[1] and r[1] > 0]
    success_count = sum(1 for r in rows if r[0] == 1)
    success_rate = (success_count / len(rows)) * 100
    avg_rt = int(statistics.mean(rts)) if rts else 0
    min_rt = min(rts) if rts else 0
    max_rt = max(rts) if rts else 0
    if success_rate > 95:
        status = "🟢 ОНЛАЙН"
    elif success_rate > 80:
        status = "🟡 НЕСТАБИЛЕН"
    else:
        status = "🔴 ОФЛАЙН"
    await m.answer(f"📊 СТАТИСТИКА @{botname}\n\nСтатус: {status}\nУспешность: {success_rate:.1f}%\nСреднее время: {avg_rt} мс\nМинимум: {min_rt} мс\nМаксимум: {max_rt} мс\nПроверок: {len(rows)}")

@router.message(Command("add"))
async def cmd_add(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /add @username")
        return
    botname = args[1].lstrip("@")
    await add_watched(botname)
    await m.answer(f"👁️ @{botname} добавлен в список фонового мониторинга (проверка каждые {MONITOR_INTERVAL} сек)")

@router.message(Command("remove"))
async def cmd_remove(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /remove @username")
        return
    botname = args[1].lstrip("@")
    await remove_watched(botname)
    await m.answer(f"👁️‍🗨️ @{botname} удалён из списка фонового мониторинга")

@router.message(Command("list"))
async def cmd_list(m: Message):
    watched_list = await get_watched_list()
    if not watched_list:
        await m.answer("📋 Список отслеживаемых ботов пуст. Добавьте бота командой /add @bot")
        return
    text = "📋 **Список отслеживаемых ботов:**\n\n"
    for i, botname in enumerate(watched_list, 1):
        text += f"{i}. @{botname}\n"
    await m.answer(text, parse_mode="Markdown")

# ======================
# ЗАПУСК
# ======================

async def shutdown():
    global db, shutdown_in_progress
    if shutdown_in_progress: return
    shutdown_in_progress = True
    logger.info("Завершение работы...")
    shutdown_event.set()
    if client: await client.disconnect()
    if bot: await bot.close()
    if db: await db.close()
    logger.info("Анализатор остановлен")

async def main():
    global bot, client, db
    if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
        logger.error("Ошибка: не все переменные окружения заданы в .env")
        return
    await init_db()
    await refresh_watched_cache()
    bot = Bot(token=BOT_TOKEN)
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Telethon не авторизован")
        return
    logger.info("Telethon подключён")
    await setup_global_handlers()
    # Запускаем фоновый мониторинг
    asyncio.create_task(background_monitor())
    # Запускаем веб-сервер для health check
    asyncio.create_task(start_web_server())
    dp = Dispatcher()
    dp.include_router(router)
    async def graceful_shutdown():
        await shutdown()
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(graceful_shutdown()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(graceful_shutdown()))
    except NotImplementedError:
        pass
    logger.info("Анализатор ботов готов. Фоновый мониторинг активен. Health check на порту %s", WEB_PORT)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
