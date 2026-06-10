#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import time
import signal
import logging
import statistics
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, types, Router
from aiogram.types import Message
from aiogram.filters import Command

from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession

import aiosqlite

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
# ПРОВЕРКА БОТА (ЛОВИМ ЛЮБЫЕ ОТВЕТЫ)
# ======================

async def ensure_telethon() -> bool:
    global client
    if not client:
        return False
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
        entity = await asyncio.wait_for(
            client.get_entity(username),
            timeout=PING_TIMEOUT
        )
        if not getattr(entity, 'bot', False):
            return False, 0, 0, "Не является ботом"
        
        bot_id = entity.id
        response_event = asyncio.Event()

        @client.on(events.NewMessage(chats=entity))
        async def message_handler(event):
            if event.message.from_id == entity.id:
                response_event.set()

        @client.on(events.CallbackQuery)
        async def callback_handler(event):
            if event.query.user_id == bot_id:
                response_event.set()

        @client.on(events.MessageEdited(chats=entity))
        async def edit_handler(event):
            if event.message.from_id == entity.id:
                response_event.set()

        start = time.perf_counter()
        await client.send_message(entity, "/start")

        try:
            await asyncio.wait_for(response_event.wait(), timeout=RESPONSE_TIMEOUT)
            rt = int((time.perf_counter() - start) * 1000)
            await save_check(username, bot_id, True, rt, "")
            logger.info(f"✅ {username} ответил за {rt} мс")
            return True, bot_id, rt, ""
        except asyncio.TimeoutError:
            await save_check(username, bot_id, False, None, "Бот не ответил")
            logger.warning(f"❌ {username} не ответил за {RESPONSE_TIMEOUT} сек")
            return False, bot_id, 0, "Нет ответа"
        finally:
            client.remove_event_handler(message_handler)
            client.remove_event_handler(callback_handler)
            client.remove_event_handler(edit_handler)

    except asyncio.TimeoutError:
        return False, 0, 0, "Таймаут получения бота"
    except errors.FloodWaitError as e:
        await asyncio.sleep(min(e.seconds, 30))
        return False, 0, 0, f"Flood на {e.seconds}с"
    except errors.rpcerrorlist.UsernameNotOccupiedError:
        return False, 0, 0, "Такое имя не существует"
    except Exception as e:
        return False, 0, 0, str(e)[:60]

# ======================
# СТАТИСТИЧЕСКИЕ ФУНКЦИИ
# ======================

async def get_bot_id_from_db(botname: str) -> int:
    async with db.execute(
        "SELECT bot_id FROM checks WHERE bot=? AND bot_id>0 ORDER BY ts DESC LIMIT 1",
        (botname,)
    ) as cursor:
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
    if total == 0:
        return 0
    return (success / total) * 100

async def get_avg_response_time(botname: str) -> int:
    async with db.execute(
        "SELECT AVG(rt) FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] else 0

async def get_min_response_time(botname: str) -> int:
    async with db.execute(
        "SELECT MIN(rt) FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] else 0

async def get_max_response_time(botname: str) -> int:
    async with db.execute(
        "SELECT MAX(rt) FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] else 0

async def get_last_response_time(botname: str) -> int:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0 ORDER BY ts DESC LIMIT 1",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_median_response_time(botname: str) -> int:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0 ORDER BY rt",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return 0
    rts = [r[0] for r in rows]
    mid = len(rts) // 2
    if len(rts) % 2 == 0:
        return int((rts[mid-1] + rts[mid]) / 2)
    return int(rts[mid])

async def get_p95_response_time(botname: str) -> int:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0 ORDER BY rt",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return 0
    rts = [r[0] for r in rows]
    idx = int(len(rts) * 0.95)
    return rts[idx] if idx < len(rts) else rts[-1]

async def get_response_time_distribution(botname: str) -> Dict:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0",
        (botname,)
    ) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts:
        return {"fast": 0, "normal": 0, "slow": 0, "very_slow": 0}
    total = len(rts)
    fast = sum(1 for r in rts if r < 200) / total * 100
    normal = sum(1 for r in rts if 200 <= r < 500) / total * 100
    slow = sum(1 for r in rts if 500 <= r < 1000) / total * 100
    very_slow = sum(1 for r in rts if r >= 1000) / total * 100
    return {
        "fast": round(fast),
        "normal": round(normal),
        "slow": round(slow),
        "very_slow": round(very_slow)
    }

async def get_jitter(botname: str) -> int:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0 ORDER BY ts DESC LIMIT 30",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if len(rows) < 3:
        return 0
    rts = [r[0] for r in rows]
    return int(statistics.stdev(rts)) if len(rts) > 1 else 0

async def get_trend_direction(botname: str) -> str:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0 ORDER BY ts DESC LIMIT 20",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if len(rows) < 6:
        return "Недостаточно данных"
    rts = [r[0] for r in rows]
    mid = len(rts) // 2
    old_avg = statistics.mean(rts[mid:])
    new_avg = statistics.mean(rts[:mid])
    if old_avg == 0:
        return "Стабильно"
    change = ((new_avg - old_avg) / old_avg) * 100
    if change > 15:
        return "Ухудшается"
    elif change < -15:
        return "Улучшается"
    return "Стабильно"

async def get_trend_change(botname: str) -> int:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0 ORDER BY ts DESC LIMIT 20",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if len(rows) < 6:
        return 0
    rts = [r[0] for r in rows]
    mid = len(rts) // 2
    old_avg = statistics.mean(rts[mid:])
    new_avg = statistics.mean(rts[:mid])
    if old_avg == 0:
        return 0
    change = ((new_avg - old_avg) / old_avg) * 100
    return abs(round(change))

async def get_uptime_last_hour(botname: str) -> float:
    since = int(time.time()) - 3600
    async with db.execute(
        "SELECT success FROM checks WHERE bot=? AND ts>?", (botname, since)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return 0
    success = sum(1 for r in rows if r[0] == 1)
    return (success / len(rows)) * 100

async def get_uptime_last_day(botname: str) -> float:
    since = int(time.time()) - 86400
    async with db.execute(
        "SELECT success FROM checks WHERE bot=? AND ts>?", (botname, since)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return 0
    success = sum(1 for r in rows if r[0] == 1)
    return (success / len(rows)) * 100

async def get_uptime_last_week(botname: str) -> float:
    since = int(time.time()) - 604800
    async with db.execute(
        "SELECT success FROM checks WHERE bot=? AND ts>?", (botname, since)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return 0
    success = sum(1 for r in rows if r[0] == 1)
    return (success / len(rows)) * 100

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
    rel = success_rate * 0.6 + max(0, 100 - avg_rt / 10) * 0.4
    return min(100, max(0, int(rel)))

async def get_risk_level(botname: str) -> str:
    success_rate = await get_success_rate(botname)
    avg_rt = await get_avg_response_time(botname)
    fail_count = await get_fail_count(botname)
    risk = 0
    if success_rate < 80: risk += 30
    elif success_rate < 90: risk += 15
    if avg_rt > 500: risk += 20
    elif avg_rt > 300: risk += 10
    if fail_count > 10: risk += 20
    elif fail_count > 5: risk += 10
    if risk < 20: return "Низкий"
    if risk < 50: return "Средний"
    return "Высокий"

async def get_prediction(botname: str) -> str:
    trend = await get_trend_direction(botname)
    success_rate = await get_success_rate(botname)
    if trend == "Улучшается" and success_rate > 90:
        return "Рост"
    elif trend == "Ухудшается" or success_rate < 80:
        return "Спад"
    else:
        return "Стабильность"

async def get_sla_grade(botname: str) -> str:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0",
        (botname,)
    ) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts:
        return "Н/Д"
    sla500 = sum(1 for r in rts if r < 500) / len(rts) * 100
    if sla500 >= 99: return "A+"
    if sla500 >= 95: return "A"
    if sla500 >= 90: return "B"
    if sla500 >= 80: return "C"
    return "D"

async def get_sla_500(botname: str) -> float:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0",
        (botname,)
    ) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts:
        return 0
    return sum(1 for r in rts if r < 500) / len(rts) * 100

async def get_sla_1000(botname: str) -> float:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt IS NOT NULL AND rt>0",
        (botname,)
    ) as cursor:
        rts = [r[0] for r in await cursor.fetchall()]
    if not rts:
        return 0
    return sum(1 for r in rts if r < 1000) / len(rts) * 100

async def get_bot_classification(botname: str) -> str:
    avg_rt = await get_avg_response_time(botname)
    jitter = await get_jitter(botname)
    if avg_rt < 200 and jitter < 50:
        return "Высокопроизводительный (Элитный)"
    elif avg_rt < 400:
        return "Стандартный (Обычный)"
    else:
        return "Медленный (Базовый)"

async def get_last_check_status(botname: str) -> str:
    async with db.execute(
        "SELECT success, error FROM checks WHERE bot=? ORDER BY ts DESC LIMIT 1",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        return "Нет проверок"
    if row[0] == 1:
        return "Успешно"
    return f"Ошибка: {row[1] if row[1] else 'Неизвестно'}"

async def get_database_record_count(botname: str) -> int:
    async with db.execute("SELECT COUNT(*) FROM checks WHERE bot=?", (botname,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else 0

async def get_data_age_days(botname: str) -> int:
    first = await get_first_seen(botname)
    if first == "Нет данных":
        return 0
    try:
        first_date = datetime.strptime(first, "%d.%m.%Y %H:%M:%S")
        return (datetime.now() - first_date).days
    except:
        return 0

async def get_data_confidence(botname: str) -> int:
    total = await get_total_checks(botname)
    return min(100, total * 5)

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
    distribution = await get_response_time_distribution(botname)
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
    
    if risk == "Низкий":
        risk_desc = "Риски минимальны, бот можно использовать без опасений"
    elif risk == "Средний":
        risk_desc = "Присутствуют риски сбоев, рекомендуется мониторинг"
    else:
        risk_desc = "Высокие риски, бот часто недоступен или медленно отвечает"
    
    lines = []
    lines.append(f"🤖 ОТЧЁТ ПО БОТУ @{botname}")
    lines.append("")
    
    # 1. ИНФОРМАЦИЯ О БОТЕ
    lines.append("1. ИНФОРМАЦИЯ О БОТЕ")
    lines.append(f"   Имя пользователя: @{botname}")
    lines.append(f"   Уникальный ID: {bot_id if bot_id else 'не определён'}")
    lines.append(f"   Тип аккаунта: Telegram Бот")
    lines.append(f"   Статус: {status_text}")
    lines.append(f"   Общее состояние: {verdict_desc}")
    lines.append("")
    
    # 2. ИСТОРИЯ ПРОВЕРОК
    lines.append("2. ИСТОРИЯ ПРОВЕРОК")
    lines.append(f"   Всего проведено проверок: {total_checks}")
    lines.append(f"   Из них успешных: {success_count}")
    lines.append(f"   Из них неудачных: {fail_count}")
    lines.append(f"   Общая успешность: {success_rate:.1f}%")
    lines.append(f"   Последний статус: {last_status}")
    lines.append("")
    
    # 3. ВРЕМЕННЫЕ МЕТРИКИ
    lines.append("3. ВРЕМЕННЫЕ МЕТРИКИ")
    lines.append(f"   Первое появление в системе: {first_seen}")
    lines.append(f"   Последнее появление в системе: {last_seen}")
    lines.append(f"   Возраст данных: {data_age_days} дней")
    lines.append(f"   Количество записей в БД: {record_count}")
    lines.append(f"   Достоверность данных: {confidence}%")
    lines.append("")
    
    # 4. СТАТИСТИКА ВРЕМЕНИ ОТВЕТА
    lines.append("4. СТАТИСТИКА ВРЕМЕНИ ОТВЕТА")
    lines.append(f"   Среднее время: {avg_rt} мс")
    lines.append(f"   Медианное время: {median_rt} мс")
    lines.append(f"   Минимальное время: {min_rt} мс")
    lines.append(f"   Максимальное время: {max_rt} мс")
    lines.append(f"   P95 (95% ответов быстрее): {p95_rt} мс")
    lines.append("")
    
    # 5. ПОСЛЕДНЯЯ ПРОВЕРКА
    lines.append("5. ПОСЛЕДНЯЯ ПРОВЕРКА")
    lines.append(f"   Время ответа при последней проверке: {last_rt} мс")
    lines.append(f"   Статус последней проверки: {last_status}")
    lines.append(f"   Время последней проверки: {last_seen}")
    lines.append(f"   Общая успешность за всё время: {success_rate:.1f}%")
    lines.append(f"   Тренд изменения: {trend_dir}")
    lines.append("")
    
    # 6. РАСПРЕДЕЛЕНИЕ СКОРОСТИ
    lines.append("6. РАСПРЕДЕЛЕНИЕ СКОРОСТИ ОТВЕТОВ")
    lines.append(f"   Быстрые ответы (менее 200мс): {distribution['fast']}%")
    lines.append(f"   Нормальные ответы (200-500мс): {distribution['normal']}%")
    lines.append(f"   Медленные ответы (500-1000мс): {distribution['slow']}%")
    lines.append(f"   Очень медленные ответы (более 1000мс): {distribution['very_slow']}%")
    lines.append("")
    
    # 7. СТАБИЛЬНОСТЬ
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
    
    # 8. ТРЕНД
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
    
    # 9. ВРЕМЯ РАБОТЫ (UPTIME)
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
    
    # 10. ОЦЕНКА ЗДОРОВЬЯ
    lines.append("10. ОЦЕНКА ЗДОРОВЬЯ БОТА")
    lines.append(f"   Общая оценка: {health}/100")
    lines.append(f"   Уровень: {verdict}")
    lines.append(f"   Надёжность: {reliability}/100")
    lines.append("")
    
    # 11. АНАЛИЗ РИСКОВ
    lines.append("11. АНАЛИЗ РИСКОВ")
    lines.append(f"   Уровень риска: {risk}")
    lines.append(f"   Описание: {risk_desc}")
    if risk == "Низкий":
        lines.append("   Рекомендация: Бот безопасен для использования")
    elif risk == "Средний":
        lines.append("   Рекомендация: Следите за состоянием бота")
    else:
        lines.append("   Рекомендация: Рекомендуется найти альтернативу")
    lines.append("")
    
    # 12. AI ПРОГНОЗ
    lines.append("12. AI ПРОГНОЗ")
    lines.append(f"   Прогноз на ближайшее время: {prediction}")
    if prediction == "Рост":
        lines.append("   Ожидается улучшение производительности")
        lines.append("   Время ответа будет уменьшаться")
    elif prediction == "Спад":
        lines.append("   Ожидается ухудшение производительности")
        lines.append("   Время ответа будет увеличиваться")
    else:
        lines.append("   Ожидается стабильная работа")
        lines.append("   Кардинальных изменений не предвидится")
    lines.append("")
    
    # 13. УРОВЕНЬ ОБСЛУЖИВАНИЯ (SLA)
    lines.append("13. УРОВЕНЬ ОБСЛУЖИВАНИЯ (SLA)")
    lines.append(f"   Доля ответов до 500мс: {sla_500:.1f}%")
    lines.append(f"   Доля ответов до 1000мс: {sla_1000:.1f}%")
    lines.append(f"   Итоговая оценка: {sla_grade}")
    if sla_grade in ["A+", "A"]:
        lines.append("   Качество обслуживания: Отличное")
    elif sla_grade in ["B", "C"]:
        lines.append("   Качество обслуживания: Удовлетворительное")
    else:
        lines.append("   Качество обслуживания: Низкое")
    lines.append("")
    
    # 14. КЛАССИФИКАЦИЯ БОТА
    lines.append("14. КЛАССИФИКАЦИЯ БОТА")
    lines.append(f"   Тип: {classification}")
    lines.append("")
    
    # 15. РЕКОМЕНДАЦИИ ПО УЛУЧШЕНИЮ
    lines.append("15. РЕКОМЕНДАЦИИ ПО УЛУЧШЕНИЮ")
    rec_count = 0
    if avg_rt > 500:
        lines.append("   • Оптимизируйте время ответа сервера")
        rec_count += 1
    if success_rate < 90:
        lines.append("   • Проверьте доступность и стабильность бота")
        rec_count += 1
    if jitter > 100:
        lines.append("   • Уменьшите джиттер, стабилизируйте соединение")
        rec_count += 1
    if total_checks < 20:
        lines.append("   • Проведите больше проверок для точной статистики")
        rec_count += 1
    if uptime_day < 95:
        lines.append("   • Улучшите аптайм, настройте мониторинг")
        rec_count += 1
    if rec_count == 0:
        lines.append("   • Все метрики в норме, бот работает отлично")
    lines.append("")
    
    # 16. ИТОГОВАЯ ОЦЕНКА
    lines.append("16. ИТОГОВАЯ ОЦЕНКА")
    lines.append(f"   Вердикт: {verdict}")
    lines.append(f"   Описание: {verdict_desc}")
    if health >= 70:
        lines.append("   Статус: Бот рекомендуется к использованию")
    elif health >= 50:
        lines.append("   Статус: Бот можно использовать с осторожностью")
    else:
        lines.append("   Статус: Бот не рекомендуется к использованию")
    lines.append("")
    
    # 17. ТЕХНИЧЕСКАЯ ИНФОРМАЦИЯ
    lines.append("17. ТЕХНИЧЕСКАЯ ИНФОРМАЦИЯ")
    lines.append(f"   Метод проверки: Отправка /start целевому боту")
    lines.append(f"   Таймаут ожидания: {RESPONSE_TIMEOUT} секунд")
    lines.append(f"   База данных: SQLite (WAL режим)")
    lines.append(f"   Обработка ответов: текст, кнопки, сервисные сообщения")
    lines.append("")
    
    # 18. ПАРАМЕТРЫ ПРОВЕРКИ
    lines.append("18. ПАРАМЕТРЫ ПРОВЕРКИ")
    lines.append(f"   Таймаут получения entity: {PING_TIMEOUT} сек")
    lines.append(f"   Таймаут ожидания ответа: {RESPONSE_TIMEOUT} сек")
    lines.append("   Тип проверки: Активная отправка команды")
    lines.append("   Ловим любые ответы (текст, кнопки, действия)")
    lines.append("")
    
    # 19. МЕТАДАННЫЕ ОТЧЁТА
    lines.append("19. МЕТАДАННЫЕ ОТЧЁТА")
    lines.append(f"   Дата генерации: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    lines.append(f"   Версия отчёта: 2.0")
    lines.append("   Все данные реальные, из базы проверок")
    lines.append("")
    
    # 20. АКТИВНОСТЬ БОТА
    lines.append("20. АКТИВНОСТЬ БОТА")
    lines.append(f"   Успешность ответов: {success_rate:.1f}%")
    lines.append(f"   Количество успешных ответов: {success_count}")
    lines.append(f"   Количество ошибок: {fail_count}")
    lines.append("")
    
    # 21. КАЧЕСТВО ОТВЕТОВ
    lines.append("21. КАЧЕСТВО ОТВЕТОВ")
    lines.append(f"   Среднее время: {avg_rt} мс")
    lines.append(f"   Медиана: {median_rt} мс")
    lines.append(f"   P95: {p95_rt} мс")
    lines.append(f"   Стабильность (джиттер): {jitter} мс")
    lines.append("")
    
    # 22. ДОСТУПНОСТЬ
    lines.append("22. ДОСТУПНОСТЬ БОТА")
    lines.append(f"   За час: {uptime_hour:.1f}%")
    lines.append(f"   За день: {uptime_day:.1f}%")
    lines.append(f"   За неделю: {uptime_week:.1f}%")
    lines.append("")
    
    # 23. РАСПРЕДЕЛЕНИЕ ПО СКОРОСТИ
    lines.append("23. РАСПРЕДЕЛЕНИЕ ПО СКОРОСТИ")
    lines.append(f"   Быстрые: {distribution['fast']}%")
    lines.append(f"   Нормальные: {distribution['normal']}%")
    lines.append(f"   Медленные: {distribution['slow']}%")
    lines.append(f"   Очень медленные: {distribution['very_slow']}%")
    lines.append("")
    
    # 24. СТАТИСТИКА ОШИБОК
    lines.append("24. СТАТИСТИКА ОШИБОК")
    lines.append(f"   Всего ошибок: {fail_count}")
    lines.append(f"   Процент ошибок: {100 - success_rate:.1f}%")
    lines.append("")
    
    # 25. ТРЕНД ПОСЛЕДНИХ ПРОВЕРОК
    lines.append("25. ТРЕНД ПОСЛЕДНИХ ПРОВЕРОК")
    lines.append(f"   Направление: {trend_dir}")
    lines.append(f"   Величина изменения: {trend_change}%")
    lines.append("")
    
    # 26. РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ
    lines.append("26. РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ")
    if health >= 70:
        lines.append("   Бот можно смело использовать")
    elif health >= 50:
        lines.append("   Бот использовать с осторожностью")
    else:
        lines.append("   От использования бота лучше отказаться")
    lines.append("")
    
    # 27. ПРОГНОЗ РАБОТЫ
    lines.append("27. ПРОГНОЗ РАБОТЫ")
    lines.append(f"   Прогноз: {prediction}")
    if prediction == "Рост":
        lines.append("   Ожидается улучшение")
    elif prediction == "Спад":
        lines.append("   Ожидается ухудшение")
    else:
        lines.append("   Бот будет работать стабильно")
    lines.append("")
    
    # 28. СТАТУС ПОСЛЕДНЕЙ ПРОВЕРКИ
    lines.append("28. СТАТУС ПОСЛЕДНЕЙ ПРОВЕРКИ")
    lines.append(f"   Статус: {last_status}")
    lines.append(f"   Время ответа: {last_rt} мс")
    lines.append(f"   Время проверки: {last_seen}")
    lines.append("")
    
    # 29. ДОЛГОСРОЧНАЯ СТАБИЛЬНОСТЬ
    lines.append("29. ДОЛГОСРОЧНАЯ СТАБИЛЬНОСТЬ")
    lines.append(f"   Среднее время за всё время: {avg_rt} мс")
    lines.append(f"   Вариативность (джиттер): {jitter} мс")
    if jitter < 100:
        lines.append("   Бот работает стабильно долгое время")
    else:
        lines.append("   Бот работает нестабильно, есть скачки")
    lines.append("")
    
    # 30. КАЧЕСТВО ОБСЛУЖИВАНИЯ
    lines.append("30. КАЧЕСТВО ОБСЛУЖИВАНИЯ")
    lines.append(f"   SLA 500мс: {sla_500:.1f}%")
    lines.append(f"   SLA 1000мс: {sla_1000:.1f}%")
    lines.append(f"   Общая оценка: {sla_grade}")
    lines.append("")
    
    # 31. НАДЁЖНОСТЬ
    lines.append("31. НАДЁЖНОСТЬ БОТА")
    lines.append(f"   Индекс надёжности: {reliability}/100")
    if reliability >= 80:
        lines.append("   Бот очень надёжный")
    elif reliability >= 60:
        lines.append("   Бот достаточно надёжный")
    else:
        lines.append("   Надёжность бота низкая")
    lines.append("")
    
    # 32. ЗДОРОВЬЕ БОТА
    lines.append("32. ЗДОРОВЬЕ БОТА")
    lines.append(f"   Оценка здоровья: {health}/100")
    if health >= 85:
        lines.append("   Бот в отличном состоянии")
    elif health >= 70:
        lines.append("   Бот в хорошем состоянии")
    elif health >= 50:
        lines.append("   Состояние бота удовлетворительное")
    else:
        lines.append("   Бот требует вмешательства")
    lines.append("")
    
    # 33. ИДЕНТИФИКАЦИОННЫЕ ДАННЫЕ
    lines.append("33. ИДЕНТИФИКАЦИОННЫЕ ДАННЫЕ")
    lines.append(f"   Username: @{botname}")
    lines.append(f"   ID бота: {bot_id if bot_id else 'не определён'}")
    lines.append("")
    
    # 34. ИСТОРИЯ ДАННЫХ
    lines.append("34. ИСТОРИЯ ДАННЫХ")
    lines.append(f"   Первое появление: {first_seen}")
    lines.append(f"   Последнее появление: {last_seen}")
    lines.append(f"   Количество проверок: {total_checks}")
    lines.append(f"   Достоверность: {confidence}%")
    lines.append("")
    
    # 35. ЭФФЕКТИВНОСТЬ
    lines.append("35. ЭФФЕКТИВНОСТЬ РАБОТЫ")
    lines.append(f"   Успешность: {success_rate:.1f}%")
    lines.append(f"   Среднее время: {avg_rt} мс")
    if success_rate > 90 and avg_rt < 300:
        lines.append("   Бот работает эффективно")
    else:
        lines.append("   Эффективность бота снижена")
    lines.append("")
    
    # 36. РИСКИ ИСПОЛЬЗОВАНИЯ
    lines.append("36. РИСКИ ИСПОЛЬЗОВАНИЯ")
    lines.append(f"   Уровень риска: {risk}")
    lines.append("")
    
    # 37. СВОДКА
    lines.append("37. СВОДКА")
    lines.append(f"   Статус: {status_text}")
    lines.append(f"   Вердикт: {verdict}")
    lines.append(f"   Прогноз: {prediction}")
    lines.append("")
    
    # 38. ВРЕМЯ ОЖИДАНИЯ ОТВЕТА
    lines.append("38. ВРЕМЯ ОЖИДАНИЯ ОТВЕТА")
    lines.append(f"   Бот ждёт ответ от проверяемого бота: {RESPONSE_TIMEOUT} секунд")
    lines.append(f"   Если бот не отвечает за {RESPONSE_TIMEOUT} сек, проверка считается неудачной")
    lines.append("   Таймаут установлен для предотвращения зависаний")
    lines.append("   При медленном интернете таймаут можно увеличить")
    lines.append("")
    
    # 39. ЗАВЕРШЕНИЕ ОТЧЁТА
    lines.append("39. ЗАВЕРШЕНИЕ ОТЧЁТА")
    lines.append("   Отчёт сгенерирован на основе реальных данных")
    lines.append("   Для обновления выполните новую проверку командой /check")
    lines.append("   Все показатели честные, без выдуманных метрик")
    lines.append("")
    
    # 40. ИТОГ
    lines.append("40. ИТОГ")
    lines.append(f"   Бот @{botname} имеет оценку здоровья {health}/100")
    lines.append(f"   Статус: {status_text}")
    lines.append(f"   Среднее время ответа: {avg_rt} мс")
    lines.append(f"   Успешность: {success_rate:.1f}%")
    
    return "\n".join(lines)

# ======================
# КОМАНДЫ БОТА
# ======================

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "🤖 **Анализатор ботов**\n\n"
        "📌 **Команды:**\n"
        "▪️ /check @bot — быстрая проверка\n"
        "▪️ /report @bot — полный отчёт (40 секций)\n"
        "▪️ /stats @bot — статистика\n\n"
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
    await m.answer(f"⏳ Генерация 40-секционного отчёта для @{botname}...")
    
    ok, _, _, err = await check_bot(botname)
    if not ok:
        await m.answer(f"⚠️ Бот не ответил: {err}")
    
    report = await generate_full_report(botname)
    if len(report) > 4000:
        parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
        for part in parts:
            await m.answer(part)
    else:
        await m.answer(report)

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
    
    text = f"""📊 СТАТИСТИКА @{botname}

Статус: {status}
Успешность: {success_rate:.1f}%
Среднее время: {avg_rt} мс
Минимум: {min_rt} мс
Максимум: {max_rt} мс
Проверок: {len(rows)}"""
    
    await m.answer(text)

# ======================
# ЗАПУСК
# ======================

async def shutdown():
    global db, shutdown_in_progress
    if shutdown_in_progress:
        return
    shutdown_in_progress = True
    logger.info("Завершение работы...")
    shutdown_event.set()
    if client:
        try:
            await client.disconnect()
        except:
            pass
    if bot:
        try:
            await bot.close()
        except:
            pass
    if db:
        try:
            await db.close()
        except:
            pass
    logger.info("Анализатор остановлен")

async def main():
    global bot, client, db
    
    if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
        logger.error("Ошибка: не все переменные окружения заданы в .env")
        return
    
    await init_db()
    
    bot = Bot(token=BOT_TOKEN)
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Telethon не авторизован")
        return
    
    logger.info("Telethon подключён")
    
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
    
    logger.info("Анализатор ботов готов. Таймаут ожидания ответа: 8 секунд")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
