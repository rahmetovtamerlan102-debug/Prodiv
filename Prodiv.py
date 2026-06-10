#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import time
import signal
import logging
import statistics
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, types, Router
from aiogram.types import Message
from aiogram.filters import Command

from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession

import aiosqlite

# ======================
# КОНФИГУРАЦИЯ (только из .env)
# ======================

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

DB_PATH = "checks.db"
PING_TIMEOUT = 10          # таймаут на получение entity
RESPONSE_TIMEOUT = 8       # сколько ждать ответа бота на /start

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
router = Router()

# ======================
# БАЗА ДАННЫХ (только история проверок)
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
# ПРОВЕРКА БОТА (реальный ответ и получение ID)
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
    """
    Возвращает (успех, реальный_id_бота, время_ответа_мс, сообщение_об_ошибке)
    успех = True только если бот прислал ответ на /start
    """
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
        async def handler(event):
            if event.message.from_id == entity.id:
                response_event.set()

        start = time.perf_counter()
        await client.send_message(entity, "/start")

        try:
            await asyncio.wait_for(response_event.wait(), timeout=RESPONSE_TIMEOUT)
            rt = int((time.perf_counter() - start) * 1000)
            await save_check(username, bot_id, True, rt, "")
            return True, bot_id, rt, ""
        except asyncio.TimeoutError:
            await save_check(username, bot_id, False, None, "Бот не ответил на /start")
            return False, bot_id, 0, "Нет ответа на /start"
        finally:
            client.remove_event_handler(handler)

    except asyncio.TimeoutError:
        return False, 0, 0, "Таймаут получения entity"
    except errors.FloodWaitError as e:
        await asyncio.sleep(min(e.seconds, 30))
        return False, 0, 0, f"Flood {e.seconds}s"
    except errors.rpcerrorlist.UsernameNotOccupiedError:
        return False, 0, 0, "Имя не существует"
    except Exception as e:
        err = str(e)[:60]
        return False, 0, 0, err

# ======================
# СТАТИСТИКА ДЛЯ /stats
# ======================

async def get_stats(botname: str) -> str:
    async with db.execute(
        "SELECT success, rt FROM checks WHERE bot=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT 30",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return f"📊 Статистика @{botname}\n\nНет данных. Сделайте /check @{botname}"
    rts = [r[1] for r in rows if r[1] and r[1] > 0]
    success_count = sum(1 for r in rows if r[0] == 1)
    success_rate = (success_count / len(rows)) * 100
    if rts:
        avg_rt = int(statistics.mean(rts))
        min_rt = min(rts)
        max_rt = max(rts)
    else:
        avg_rt = min_rt = max_rt = 0
    status = "🟢 ДОСТУПЕН" if success_rate > 95 else "🟡 НЕСТАБИЛЕН" if success_rate > 80 else "🔴 НЕДОСТУПЕН"
    return (
        f"📊 СТАТИСТИКА @{botname}\n\n"
        f"Статус: {status}\n"
        f"Успешность: {success_rate:.1f}%\n"
        f"Среднее время: {avg_rt} мс\n"
        f"Минимум: {min_rt} мс\n"
        f"Максимум: {max_rt} мс\n"
        f"Проверок: {len(rows)}"
    )

# ======================
# ФУНКЦИИ ДЛЯ ОТЧЁТА (40 СЕКЦИЙ)
# ======================

async def get_bot_real_info(botname: str) -> Tuple[int, str]:
    """Возвращает (bot_id, username) из последней успешной проверки"""
    async with db.execute(
        "SELECT bot_id FROM checks WHERE bot=? AND bot_id IS NOT NULL AND bot_id>0 ORDER BY ts DESC LIMIT 1",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    if row:
        return row[0], botname
    return 0, botname

async def get_basic_info(botname: str) -> Dict:
    async with db.execute(
        "SELECT MIN(ts), MAX(ts), COUNT(*), SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) FROM checks WHERE bot=?",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    if row and row[0]:
        return {
            "first_seen": datetime.fromtimestamp(row[0]).strftime("%d.%m.%Y"),
            "last_seen": datetime.fromtimestamp(row[1]).strftime("%d.%m.%Y"),
            "total_checks": row[2],
            "fail_count": row[3] or 0
        }
    return {"first_seen": "Н/Д", "last_seen": "Н/Д", "total_checks": 0, "fail_count": 0}

async def get_performance(botname: str, limit=100) -> Dict:
    async with db.execute(
        "SELECT rt, success FROM checks WHERE bot=? AND rt IS NOT NULL ORDER BY ts DESC LIMIT ?",
        (botname, limit)
    ) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return {"avg_rt": 0, "min_rt": 0, "max_rt": 0, "median_rt": 0, "p95_rt": 0,
                "checks": 0, "success_count": 0, "success_rate": 100}
    rts = [r[0] for r in rows if r[0] and r[0] > 0]
    success_count = sum(1 for r in rows if r[1] == 1)
    if rts:
        sorted_rts = sorted(rts)
        p95 = sorted_rts[int(len(sorted_rts)*0.95)] if len(sorted_rts)>1 else sorted_rts[-1]
        return {
            "avg_rt": int(statistics.mean(rts)),
            "min_rt": min(rts),
            "max_rt": max(rts),
            "median_rt": int(statistics.median(rts)),
            "p95_rt": p95,
            "checks": len(rows),
            "success_count": success_count,
            "success_rate": (success_count/len(rows))*100
        }
    return {"avg_rt": 0, "min_rt": 0, "max_rt": 0, "median_rt": 0, "p95_rt": 0,
            "checks": len(rows), "success_count": success_count, "success_rate": 0}

async def get_uptime(botname: str, hours=24) -> float:
    since = int(time.time()) - hours*3600
    async with db.execute("SELECT success FROM checks WHERE bot=? AND ts>?", (botname, since)) as cursor:
        rows = await cursor.fetchall()
    if not rows:
        return 100.0
    return (sum(1 for r in rows if r[0]==1)/len(rows))*100

async def get_stability(botname: str) -> Dict:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 30",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if len(rows) < 3:
        return {"jitter": 0, "level": "НЕДОСТАТОЧНО ДАННЫХ", "text": "Мало данных", "cv": 0}
    rts = [r[0] for r in rows]
    jitter = int(statistics.stdev(rts)) if len(rts) > 1 else 0
    mean_rt = statistics.mean(rts)
    cv = (jitter/mean_rt)*100 if mean_rt > 0 else 0
    if jitter < 50 and cv < 20:
        return {"jitter": jitter, "level": "ВЫСОКАЯ", "text": "Отлично", "cv": round(cv, 1)}
    elif jitter < 100 and cv < 40:
        return {"jitter": jitter, "level": "ХОРОШАЯ", "text": "Хорошо", "cv": round(cv, 1)}
    elif jitter < 200:
        return {"jitter": jitter, "level": "СРЕДНЯЯ", "text": "Умеренно", "cv": round(cv, 1)}
    else:
        return {"jitter": jitter, "level": "НИЗКАЯ", "text": "Плохо", "cv": round(cv, 1)}

async def get_trend(botname: str) -> Dict:
    async with db.execute(
        "SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 30",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    if len(rows) < 6:
        return {"direction": "СТАБИЛЬНО", "change": 0, "text": "Недостаточно данных"}
    mid = len(rows)//2
    old_vals = [r[0] for r in rows[mid:]]
    new_vals = [r[0] for r in rows[:mid]]
    old_avg = statistics.mean(old_vals) if old_vals else 0
    new_avg = statistics.mean(new_vals) if new_vals else 0
    if old_avg == 0:
        return {"direction": "СТАБИЛЬНО", "change": 0, "text": "Стабильно"}
    change = ((new_avg - old_avg)/old_avg)*100
    if change > 15:
        return {"direction": "УХУДШЕНИЕ", "change": abs(round(change)), "text": "Ухудшается"}
    elif change < -15:
        return {"direction": "УЛУЧШЕНИЕ", "change": abs(round(change)), "text": "Улучшается"}
    else:
        return {"direction": "СТАБИЛЬНО", "change": abs(round(change)), "text": "Стабильно"}

async def get_health_score(botname: str) -> int:
    perf = await get_performance(botname, 50)
    stab = await get_stability(botname)
    uptime = await get_uptime(botname, 24)
    score = 100
    if perf["avg_rt"] > 1000: score -= 25
    elif perf["avg_rt"] > 500: score -= 15
    elif perf["avg_rt"] > 300: score -= 8
    elif perf["avg_rt"] > 200: score -= 3
    if perf["success_rate"] < 70: score -= 30
    elif perf["success_rate"] < 85: score -= 15
    elif perf["success_rate"] < 95: score -= 5
    if stab["jitter"] > 200: score -= 20
    elif stab["jitter"] > 100: score -= 10
    elif stab["jitter"] > 50: score -= 5
    if uptime < 90: score -= 15
    elif uptime < 95: score -= 5
    return max(0, min(100, int(score)))

# ======================
# ГЕНЕРАЦИЯ ОТЧЁТА (40 СЕКЦИЙ) С РЕАЛЬНЫМ ID
# ======================

async def generate_full_report(botname: str) -> str:
    info = await get_basic_info(botname)
    perf = await get_performance(botname, 100)
    trend = await get_trend(botname)
    stab = await get_stability(botname)
    health = await get_health_score(botname)
    uptime1 = await get_uptime(botname, 1)
    uptime24 = await get_uptime(botname, 24)
    uptime7 = await get_uptime(botname, 168)
    real_bot_id, _ = await get_bot_real_info(botname)

    if health >= 85: lvl = "ЭЛИТНЫЙ 🏆"
    elif health >= 70: lvl = "ХОРОШИЙ ✅"
    elif health >= 50: lvl = "СРЕДНИЙ ⚠️"
    else: lvl = "ПЛОХОЙ ❌"

    status = "ОНЛАЙН 🟢" if perf["success_rate"] > 95 else "ДЕГРАДИРУЕТ 🟡" if perf["success_rate"] > 80 else "ОФЛАЙН 🔴"
    fail_rate = 100 - perf["success_rate"]

    days = 0
    if info.get("first_seen") != "Н/Д":
        try:
            first = datetime.strptime(info["first_seen"], "%d.%m.%Y")
            days = (datetime.now() - first).days
        except: pass

    comp_resp = max(0, 100 - perf["avg_rt"]/10)
    comp_stab = max(0, 100 - stab["jitter"]/5)
    comp_avail = uptime24

    perf_score = 100 if perf["avg_rt"] < 200 else 80 if perf["avg_rt"] < 400 else 60 if perf["avg_rt"] < 700 else 40
    stab_score = 90 if stab["jitter"] < 50 else 70 if stab["jitter"] < 100 else 50 if stab["jitter"] < 200 else 30
    reliability = int((perf["success_rate"] * 0.5) + (max(0,100 - perf["avg_rt"]/10) * 0.3) + (max(0,100 - stab["jitter"]/5) * 0.2))
    reliability = min(100, max(0, reliability))

    quality = "НЕИЗВЕСТНО"
    if perf["avg_rt"] > 0:
        if perf["avg_rt"] < 200: quality = "ОТЛИЧНОЕ"
        elif perf["avg_rt"] < 400: quality = "ВЫСОКОЕ"
        elif perf["avg_rt"] < 700: quality = "СРЕДНЕЕ"
        else: quality = "НИЗКОЕ"

    risk_score = 0
    if perf["success_rate"] < 80: risk_score += 30
    elif perf["success_rate"] < 90: risk_score += 15
    if perf["avg_rt"] > 500: risk_score += 20
    elif perf["avg_rt"] > 300: risk_score += 10
    if stab["jitter"] > 100: risk_score += 20
    elif stab["jitter"] > 50: risk_score += 10
    fail_percent = (info["fail_count"]/max(1, info["total_checks"]))*100
    if fail_percent > 20: risk_score += 30
    elif fail_percent > 10: risk_score += 15
    risk_text = "НИЗКИЙ" if risk_score < 20 else "СРЕДНИЙ" if risk_score < 50 else "ВЫСОКИЙ"

    if trend["direction"] == "УЛУЧШЕНИЕ" and perf["success_rate"]>90 and stab["jitter"]<100:
        pred = "РОСТ"
    elif trend["direction"] == "УХУДШЕНИЕ" or perf["success_rate"]<80:
        pred = "СПАД"
    elif stab["jitter"]>150:
        pred = "НЕСТАБИЛЬНОСТЬ"
    else:
        pred = "СТАБИЛЬНОСТЬ"

    activity = 30 if info["total_checks"] < 10 else int(perf["success_rate"]*0.7 + min(100, info["total_checks"]/2)*0.3)
    activity = min(100, max(0, activity))

    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts ASC LIMIT 10", (botname,)) as cur:
        old = await cur.fetchall()
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 10", (botname,)) as cur:
        new = await cur.fetchall()
    imp_text = "Недостаточно данных"
    if len(old)>=5 and len(new)>=5:
        old_avg = statistics.mean([r[0] for r in old])
        new_avg = statistics.mean([r[0] for r in new])
        if old_avg != 0:
            imp = ((old_avg - new_avg)/old_avg)*100
            if imp > 0: imp_text = f"Улучшается ({round(abs(imp))}%)"
            elif imp < 0: imp_text = f"Ухудшается ({round(abs(imp))}%)"
            else: imp_text = "Стабильно"

    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0", (botname,)) as cur:
        all_rts = [r[0] for r in await cur.fetchall()]
    total = len(all_rts)
    if total:
        fast = sum(1 for r in all_rts if r < 200)/total*100
        norm = sum(1 for r in all_rts if 200 <= r < 500)/total*100
        slow = sum(1 for r in all_rts if 500 <= r < 1000)/total*100
        vslow = sum(1 for r in all_rts if r >= 1000)/total*100
        sla500 = sum(1 for r in all_rts if r < 500)/total*100
        sla1000 = sum(1 for r in all_rts if r < 1000)/total*100
        grade = "A+" if sla500 >= 99 else "A" if sla500 >= 95 else "B" if sla500 >= 90 else "C" if sla500 >= 80 else "D"
    else:
        fast = norm = slow = vslow = 0
        sla500 = sla1000 = 0
        grade = "Н/Д"
    best_rt = min(all_rts) if all_rts else 0
    worst_rt = max(all_rts) if all_rts else 0

    data_conf = min(100, info["total_checks"]*5)
    metric_conf = min(100, perf["checks"]*2)
    overall_conf = (data_conf + metric_conf)//2

    lines = []
    lines.append(f"🤖 ОТЧЁТ ПО БОТУ @{botname}")
    lines.append("")

    lines.append("📌 1. ИДЕНТИФИКАЦИЯ")
    lines.append(f"Имя пользователя: @{botname}")
    lines.append(f"ID бота: {real_bot_id if real_bot_id else 'неизвестно'}")
    lines.append("Тип: Telegram Бот")

    lines.append("")
    lines.append("📅 2. ВРЕМЯ ЖИЗНИ")
    lines.append(f"Первое появление: {info['first_seen']}")
    lines.append(f"Последнее появление: {info['last_seen']}")
    lines.append(f"Возраст: {days} дней")
    lines.append(f"Всего проверок: {info['total_checks']}")

    lines.append("")
    lines.append("📊 3. СТАТУС")
    lines.append(f"Текущий статус: {status}")
    lines.append(f"Успешность: {perf['success_rate']:.1f}%")
    lines.append(f"Сбоев: {info['fail_count']}")
    lines.append(f"Последняя проверка: {info['last_seen']}")

    lines.append("")
    lines.append("❤️ 4. ОЦЕНКА ЗДОРОВЬЯ")
    lines.append(f"Оценка: {health}/100")
    lines.append(f"Уровень: {lvl}")
    lines.append(f"Риск: {risk_text}")

    lines.append("")
    lines.append("⚡ 5. ПРОИЗВОДИТЕЛЬНОСТЬ (мс)")
    lines.append(f"Среднее: {perf['avg_rt']} мс")
    lines.append(f"Медиана: {perf['median_rt']} мс")
    lines.append(f"P95: {perf['p95_rt']} мс")
    lines.append(f"Минимум: {perf['min_rt']} мс")
    lines.append(f"Максимум: {perf['max_rt']} мс")

    lines.append("")
    lines.append("📈 6. УСПЕШНОСТЬ")
    lines.append(f"Успешно: {perf['success_rate']:.1f}%")
    lines.append(f"Сбоев: {fail_rate:.1f}%")
    lines.append(f"Удачных проверок: {perf['success_count']}")
    lines.append(f"Неудачных: {info['fail_count']}")

    lines.append("")
    lines.append("⏱ 7. ВРЕМЯ РАБОТЫ (UPTIME)")
    lines.append(f"Последний час: {uptime1:.1f}%")
    lines.append(f"Последние 24ч: {uptime24:.1f}%")
    lines.append(f"Последние 7 дней: {uptime7:.1f}%")

    lines.append("")
    lines.append("🧪 8. СТАБИЛЬНОСТЬ")
    lines.append(f"Джиттер: {stab['jitter']} мс")
    lines.append(f"Коэф. вариации: {stab['cv']}%")
    lines.append(f"Консистентность: {stab['text']}")
    lines.append(f"Уровень: {stab['level']}")

    lines.append("")
    lines.append("📉 9. ТРЕНД")
    lines.append(f"Направление: {trend['text']}")
    lines.append(f"Изменение: {trend['change']}%")

    lines.append("")
    lines.append("💬 10. КАЧЕСТВО ОТВЕТОВ")
    lines.append(f"Оценка: {quality}")

    lines.append("")
    lines.append("🛡 11. ИНДЕКС НАДЁЖНОСТИ")
    lines.append(f"Оценка: {reliability}/100")

    lines.append("")
    lines.append("⚠️ 12. АНАЛИЗ РИСКОВ")
    lines.append(f"Уровень: {risk_text}")

    lines.append("")
    lines.append("🔮 13. AI ПРОГНОЗ")
    lines.append(f"Прогноз: {pred}")

    lines.append("")
    lines.append("📡 14. АКТИВНОСТЬ")
    lines.append(f"Оценка: {activity}/100")

    lines.append("")
    lines.append("📊 15. ДИНАМИКА УЛУЧШЕНИЙ")
    lines.append(imp_text)

    lines.append("")
    lines.append("🏆 16. ГЛОБАЛЬНЫЙ РЕЙТИНГ")
    lines.append("(требуется больше данных)")

    lines.append("")
    lines.append("🤖 17. КЛАССИФИКАЦИЯ БОТА")
    if perf['avg_rt'] < 200 and stab['jitter'] < 50:
        lines.append("Тип: Высокопроизводительный")
        lines.append("Категория: Элитный")
    elif perf['avg_rt'] < 400:
        lines.append("Тип: Стандартный")
        lines.append("Категория: Обычный")
    else:
        lines.append("Тип: Медленный")
        lines.append("Категория: Базовый")

    lines.append("")
    lines.append("💾 18. МЕТРИКИ БАЗЫ ДАННЫХ")
    lines.append(f"Записей: {info['total_checks']}")
    lines.append(f"Глубина данных: {days} дней")
    lines.append(f"Размер выборки: {perf['checks']}")

    lines.append("")
    lines.append("⏰ 19. ЧАСЫ ПИК")
    lines.append("(требуется больше данных)")

    lines.append("")
    lines.append("📆 20. НЕДЕЛЬНАЯ АКТИВНОСТЬ")
    lines.append("(требуется больше данных)")

    lines.append("")
    lines.append("📈 21. КРАТКОСРОЧНЫЙ ТРЕНД")
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 5", (botname,)) as cur:
        last5 = await cur.fetchall()
    if len(last5) >= 2:
        chg = ((last5[0][0] - last5[-1][0])/max(1, last5[-1][0]))*100
        if chg < 0: lines.append(f"Изменение: {chg:+.1f}% 📈 Улучшается")
        elif chg > 0: lines.append(f"Изменение: {chg:+.1f}% 📉 Ухудшается")
        else: lines.append("Изменение: 0% Стабильно")
    else:
        lines.append("Недостаточно данных")

    lines.append("")
    lines.append("📉 22. ДОЛГОСРОЧНЫЙ ТРЕНД")
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts ASC LIMIT 10", (botname,)) as cur:
        first10 = await cur.fetchall()
    async with db.execute("SELECT rt FROM checks WHERE bot=? AND rt>0 ORDER BY ts DESC LIMIT 10", (botname,)) as cur:
        last10 = await cur.fetchall()
    if len(first10) >= 5 and len(last10) >= 5:
        first_avg = statistics.mean([r[0] for r in first10])
        last_avg = statistics.mean([r[0] for r in last10])
        chg = ((last_avg - first_avg)/first_avg)*100 if first_avg else 0
        if chg < 0: lines.append(f"Изменение: {chg:+.1f}% 📈 Улучшается")
        elif chg > 0: lines.append(f"Изменение: {chg:+.1f}% 📉 Ухудшается")
        else: lines.append("Изменение: 0% Стабильно")
    else:
        lines.append("Недостаточно данных")

    lines.append("")
    lines.append("🎯 23. ЦЕЛЕВЫЕ МЕТРИКИ")
    target_rt = 200 if perf['avg_rt'] > 200 else perf['avg_rt']
    lines.append(f"Целевое время: <{target_rt} мс")
    lines.append(f"Текущее время: {perf['avg_rt']} мс")
    lines.append(f"Отставание: {max(0, perf['avg_rt'] - target_rt)} мс")

    lines.append("")
    lines.append("❤️ 24. КОМПОНЕНТЫ ЗДОРОВЬЯ")
    lines.append(f"Скорость: {int(comp_resp)}/100")
    lines.append(f"Стабильность: {int(min(100, comp_stab))}/100")
    lines.append(f"Доступность: {int(comp_avail)}/100")

    lines.append("")
    lines.append("❌ 25. ДЕТАЛИЗАЦИЯ ОШИБОК")
    lines.append(f"Процент сбоев: {fail_rate:.1f}%")
    if fail_rate > 20: lines.append("Тренд ошибок: 📈 Растёт")
    elif fail_rate < 5: lines.append("Тренд ошибок: 📉 Падает")
    else: lines.append("Тренд ошибок: Стабильный")

    lines.append("")
    lines.append("📜 26. ИСТОРИЯ ПРОИЗВОДИТЕЛЬНОСТИ")
    lines.append(f"Лучший результат: {best_rt} мс")
    lines.append(f"Худший результат: {worst_rt} мс")
    lines.append(f"Среднее за неделю: {perf['avg_rt']} мс")

    lines.append("")
    lines.append("⚡ 27. РАСПРЕДЕЛЕНИЕ СКОРОСТИ")
    lines.append(f"Быстрые (<200мс): {fast:.0f}%")
    lines.append(f"Нормальные (200-500мс): {norm:.0f}%")
    lines.append(f"Медленные (500-1000мс): {slow:.0f}%")
    lines.append(f"Очень медленные (>1000мс): {vslow:.0f}%")

    lines.append("")
    lines.append("📊 28. УРОВЕНЬ ОБСЛУЖИВАНИЯ (SLA)")
    lines.append(f"SLA 500мс: {sla500:.1f}%")
    lines.append(f"SLA 1000мс: {sla1000:.1f}%")
    lines.append(f"Оценка SLA: {grade}")

    lines.append("")
    lines.append("🌍 29. СРАВНЕНИЕ С ДРУГИМИ БОТАМИ")
    lines.append("(требуется больше данных)")

    lines.append("")
    lines.append("📊 30. ДОВЕРИТЕЛЬНЫЙ ИНТЕРВАЛ")
    lines.append(f"Достоверность данных: {data_conf}%")
    lines.append(f"Достоверность метрик: {metric_conf}%")
    lines.append(f"Общая достоверность: {overall_conf}%")

    lines.append("")
    lines.append("📈 31. ПРОГНОЗ (7 дней)")
    if trend["direction"] == "УЛУЧШЕНИЕ":
        exp_rt = max(50, perf['avg_rt'] - perf['avg_rt']*0.1)
        outlook = "Улучшение"
    elif trend["direction"] == "УХУДШЕНИЕ":
        exp_rt = perf['avg_rt'] + perf['avg_rt']*0.1
        outlook = "Ухудшение"
    else:
        exp_rt = perf['avg_rt']
        outlook = "Стабильно"
    lines.append(f"Ожидаемое время: {int(exp_rt)} мс")
    lines.append(f"Уверенность: {70 if perf['checks'] > 20 else 40}%")
    lines.append(f"Прогноз: {outlook}")

    lines.append("")
    lines.append("💡 32. РЕКОМЕНДАЦИИ")
    recs = []
    if perf['avg_rt'] > 500: recs.append("Оптимизируйте время ответа")
    if perf['success_rate'] < 90: recs.append("Проверьте доступность бота")
    if stab['jitter'] > 100: recs.append("Уменьшите джиттер")
    if info['total_checks'] < 20: recs.append("Нужно больше данных")
    if uptime24 < 95: recs.append("Улучшите аптайм")
    if recs:
        for rec in recs[:5]: lines.append(f"• {rec}")
    else:
        lines.append("✅ Все метрики в норме")

    lines.append("")
    lines.append("📋 33. СВОДНАЯ ИНФОРМАЦИЯ")
    lines.append(f"Общая оценка: {lvl}")
    lines.append(f"Надёжность: {reliability}/100")
    if health >= 70: lines.append("Рекомендация: ✅ ИСПОЛЬЗОВАТЬ")
    elif health >= 50: lines.append("Рекомендация: ⚠️ С ОСТОРОЖНОСТЬЮ")
    else: lines.append("Рекомендация: ❌ ИЗБЕГАТЬ")

    lines.append("")
    lines.append("🏁 34. ИТОГОВАЯ ОЦЕНКА")
    lines.append(f"Здоровье: {health}/100")
    lines.append(f"Производительность: {perf_score}/100")
    lines.append(f"Стабильность: {stab_score}/100")
    lines.append(f"Общая оценка: {health}/100")

    lines.append("")
    lines.append("📊 35. БЫСТРАЯ СТАТИСТИКА")
    lines.append(f"⚡ {perf['avg_rt']} мс в среднем")
    lines.append(f"✅ {perf['success_rate']:.0f}% успешных")
    lines.append(f"📊 {stab['jitter']} мс джиттер")
    lines.append(f"🏆 {lvl}")

    lines.append("")
    lines.append("🎯 36. ВЕРДИКТ О БОТЕ")
    if health >= 85: lines.append("🔹 ОТЛИЧНЫЙ бот - настоятельно рекомендуется")
    elif health >= 70: lines.append("🔸 ХОРОШИЙ бот - рекомендуется")
    elif health >= 50: lines.append("🔸 СРЕДНИЙ бот - с осторожностью")
    else: lines.append("🔹 ПЛОХОЙ бот - не рекомендуется")

    lines.append("")
    lines.append("📅 37. МЕТАДАННЫЕ ОТЧЁТА")
    lines.append(f"Сгенерирован: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    lines.append("Источник: SQLite")
    lines.append("Версия: 6.0 (аналитический)")

    lines.append("")
    lines.append("🔧 38. ОТЛАДОЧНАЯ ИНФОРМАЦИЯ")
    lines.append(f"Проверок в БД: {info['total_checks']}")
    lines.append(f"Сырая успешность: {perf['success_rate']:.2f}%")
    lines.append("Алгоритм здоровья: v6")

    lines.append("")
    lines.append("📌 39. РЕЖИМ РАБОТЫ")
    lines.append("Проверка только по командам")
    lines.append("Автоматические проверки: ОТСУТСТВУЮТ")

    lines.append("")
    lines.append("🔚 40. КОНЕЦ ОТЧЁТА")
    lines.append("Все данные реальные, получены при отправке /start")

    return "\n".join(lines)

# ======================
# КОМАНДЫ БОТА
# ======================

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "🤖 **Анализатор ботов**\n\n"
        "📌 **Команды:**\n"
        "▪️ /check @bot — быстрая проверка (отправка /start)\n"
        "▪️ /fullreport @bot — полный отчёт (40 секций)\n"
        "▪️ /stats @bot — статистика по истории\n\n"
        "Пример: /fullreport @example_bot"
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
        msg = f"✅ @{botname} ОТВЕТИЛ\n⏱️ Время ответа: {rt} мс\n🆔 ID: {bot_id}"
        await m.answer(msg)
    else:
        await m.answer(f"❌ @{botname} НЕ ОТВЕТИЛ\n📛 Ошибка: {err}")

@router.message(Command("fullreport"))
async def cmd_fullreport(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /fullreport @username")
        return
    botname = args[1].lstrip("@")
    await m.answer(f"⏳ Проверка и генерация 40-секционного отчёта для @{botname}...")
    ok, _, _, err = await check_bot(botname)
    if not ok:
        await m.answer(f"⚠️ Бот @{botname} не ответил на /start. Отчёт будет частичным.\nОшибка: {err}")
    report = await generate_full_report(botname)
    if len(report) > 3800:
        parts = [report[i:i+3800] for i in range(0, len(report), 3800)]
        for i, part in enumerate(parts):
            if i == 0: await m.answer(part)
            else: await m.answer(part)
    else:
        await m.answer(report)

@router.message(Command("stats"))
async def cmd_stats(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /stats @username")
        return
    botname = args[1].lstrip("@")
    text = await get_stats(botname)
    await m.answer(text)

# ======================
# ЗАПУСК
# ======================

async def shutdown():
    global db
    logger.info("Завершение работы...")
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
    bot = Bot(token=BOT_TOKEN)
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Telethon не авторизован. Проверьте SESSION_STRING")
        return
    logger.info("Telethon подключён")
    dp = Dispatcher()
    dp.include_router(router)
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(shutdown()))
    except NotImplementedError:
        pass
    logger.info("Анализатор ботов готов (версия 6.0, без мониторинга)")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
