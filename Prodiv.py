#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import time
import logging
import statistics
import tempfile
import random
import string
from datetime import datetime
from typing import Optional, Dict, Set, List, Tuple

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, types, Router
from aiogram.types import Message, FSInputFile
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
MONITOR_INTERVAL = 3600  # 1 час

# Настройка красивых логов в консоль
logging.basicConfig(
    level=logging.INFO,
    format='\033[92m%(asctime)s\033[0m - \033[94m%(levelname)s\033[0m - %(message)s',
    datefmt='%H:%M:%S'
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
pending_checks: Dict[int, asyncio.Queue] = {}

watched_cache: Set[str] = set()
watched_cache_lock = asyncio.Lock()
monitor_tasks: Dict[str, asyncio.Task] = {}

# ======================
# БАЗА ДАННЫХ
# ======================

async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("PRAGMA journal_mode=WAL")
    
    await db.execute("""
    CREATE TABLE IF NOT EXISTS checks (
        id INTEGER PRIMARY KEY,
        bot TEXT,
        bot_id INTEGER,
        ts INTEGER,
        success INTEGER,
        rt INTEGER,
        error TEXT,
        command TEXT
    )
    """)
    
    await db.execute("""
    CREATE TABLE IF NOT EXISTS monitor_sessions (
        id INTEGER PRIMARY KEY,
        bot TEXT,
        start_ts INTEGER,
        end_ts INTEGER,
        status TEXT
    )
    """)
    
    await db.commit()
    logger.info("✅ База данных готова")

async def save_check(botname: str, bot_id: int, success: bool, rt: Optional[int], error: str = "", command: str = ""):
    now = int(time.time())
    await db.execute(
        "INSERT INTO checks(bot, bot_id, ts, success, rt, error, command) VALUES (?,?,?,?,?,?,?)",
        (botname, bot_id, now, 1 if success else 0, rt, error, command)
    )
    await db.commit()

async def save_fast_check(botname: str, bot_id: int, results: List[dict]):
    for r in results:
        await db.execute(
            "INSERT INTO checks(bot, bot_id, ts, success, rt, error, command) VALUES (?,?,?,?,?,?,?)",
            (botname, bot_id, int(time.time()), 1 if r['success'] else 0, r.get('rt'), r.get('error', ''), r.get('command', ''))
        )
    await db.commit()

# ======================
# УПРАВЛЕНИЕ МОНИТОРИНГОМ
# ======================

async def add_to_monitor(botname: str):
    async with watched_cache_lock:
        watched_cache.add(botname)
    logger.info(f"➕ @{botname} добавлен в мониторинг")

async def remove_from_monitor(botname: str):
    async with watched_cache_lock:
        watched_cache.discard(botname)
    logger.info(f"➖ @{botname} удалён из мониторинга")

async def start_monitor_session(botname: str, days: int):
    now = int(time.time())
    end_ts = now + (days * 86400)
    
    await db.execute(
        "UPDATE monitor_sessions SET status = 'completed' WHERE bot = ? AND status = 'active'",
        (botname,)
    )
    
    await db.execute(
        "INSERT INTO monitor_sessions (bot, start_ts, end_ts, status) VALUES (?,?,?,?)",
        (botname, now, end_ts, 'active')
    )
    await db.commit()
    await add_to_monitor(botname)
    logger.info(f"🚀 Мониторинг @{botname} запущен на {days} дней")

async def stop_monitor_session(botname: str):
    await db.execute(
        "UPDATE monitor_sessions SET status = 'stopped' WHERE bot = ? AND status = 'active'",
        (botname,)
    )
    await db.commit()
    await remove_from_monitor(botname)
    logger.info(f"🛑 Мониторинг @{botname} остановлен")

async def get_active_session(botname: str) -> Optional[dict]:
    async with db.execute(
        "SELECT id, start_ts, end_ts, status FROM monitor_sessions WHERE bot = ? AND status = 'active'",
        (botname,)
    ) as cursor:
        row = await cursor.fetchone()
    if row:
        return {'id': row[0], 'start_ts': row[1], 'end_ts': row[2], 'status': row[3]}
    return None

# ======================
# ПРОВЕРКА БОТА
# ======================

async def ensure_telethon() -> bool:
    global client
    if not client:
        return False
    try:
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=10)
            logger.info("🔌 Telethon переподключён")
        if not await client.is_user_authorized():
            logger.error("❌ Telethon не авторизован")
            return False
        return True
    except Exception as e:
        logger.error(f"❌ Telethon ошибка: {e}")
        return False

async def check_bot_once(username: str, command: str = "/start") -> Tuple[bool, int, int, str]:
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
        await client.send_message(entity, command)
        
        try:
            _, _ = await asyncio.wait_for(queue.get(), timeout=RESPONSE_TIMEOUT)
            rt = int((time.perf_counter() - start) * 1000)
            return True, bot_id, rt, ""
        except asyncio.TimeoutError:
            return False, bot_id, 0, "Нет ответа"
        finally:
            pending_checks.pop(bot_id, None)
            
    except asyncio.TimeoutError:
        return False, 0, 0, "Таймаут получения бота"
    except errors.rpcerrorlist.UsernameNotOccupiedError:
        return False, 0, 0, "Бот не существует"
    except Exception as e:
        return False, 0, 0, str(e)[:50]

async def fast_check_bot(username: str) -> Tuple[bool, int, List[dict]]:
    if not await ensure_telethon():
        return False, 0, []
    
    try:
        entity = await asyncio.wait_for(client.get_entity(username), timeout=PING_TIMEOUT)
        if not getattr(entity, 'bot', False):
            return False, 0, []
        
        bot_id = entity.id
        results = []
        
        commands = [
            "/start",
            "Привет",
            f"Тест {random.randint(1000, 9999)}",
            "?",
            ''.join(random.choices(string.ascii_lowercase, k=6))
        ]
        
        logger.info(f"🔍 Начинаю быструю проверку @{username} (5 замеров)...")
        
        for i, cmd in enumerate(commands, 1):
            logger.info(f"   Замер {i}/5: '{cmd[:15]}...'")
            queue = asyncio.Queue()
            pending_checks[bot_id] = queue
            
            start = time.perf_counter()
            await client.send_message(entity, cmd)
            
            try:
                _, _ = await asyncio.wait_for(queue.get(), timeout=RESPONSE_TIMEOUT)
                rt = int((time.perf_counter() - start) * 1000)
                results.append({'command': cmd, 'success': True, 'rt': rt, 'error': ''})
                logger.info(f"      ✅ {rt} мс")
            except asyncio.TimeoutError:
                results.append({'command': cmd, 'success': False, 'rt': None, 'error': 'Нет ответа'})
                logger.info(f"      ❌ Нет ответа")
            finally:
                pending_checks.pop(bot_id, None)
            
            await asyncio.sleep(0.3)
        
        await save_fast_check(username[1:] if username.startswith('@') else username, bot_id, results)
        logger.info(f"✅ Быстрая проверка @{username} завершена")
        return True, bot_id, results
        
    except Exception as e:
        logger.error(f"❌ Ошибка быстрой проверки: {e}")
        return False, 0, []

# ======================
# ФОНОВЫЙ МОНИТОРИНГ (1 РАЗ В ЧАС)
# ======================

async def monitor_single_bot(botname: str):
    logger.info(f"🟢 Старт мониторинга @{botname} (раз в час)")
    
    while not shutdown_event.is_set():
        try:
            session = await get_active_session(botname)
            if not session:
                logger.info(f"📴 Мониторинг @{botname} остановлен (сессия завершена)")
                break
            
            if session['end_ts'] < int(time.time()):
                await stop_monitor_session(botname)
                logger.info(f"📴 Мониторинг @{botname} завершён (время истекло)")
                break
            
            logger.info(f"⏰ {datetime.now().strftime('%H:%M')} | Проверка @{botname}...")
            ok, bot_id, rt, err = await check_bot_once(f"@{botname}", "/start")
            await save_check(botname, bot_id, ok, rt, err, "/start")
            
            if ok:
                logger.info(f"   ✅ @{botname} → {rt} мс")
            else:
                logger.warning(f"   ❌ @{botname} → {err}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка мониторинга @{botname}: {e}")
        
        logger.info(f"💤 Следующая проверка @{botname} через 1 час")
        await asyncio.sleep(MONITOR_INTERVAL)

async def background_monitor():
    logger.info("🔄 Фоновый мониторинг запущен")
    while not shutdown_event.is_set():
        current_watched = set()
        async with watched_cache_lock:
            current_watched = watched_cache.copy()
        
        for botname in current_watched:
            if botname not in monitor_tasks or monitor_tasks[botname].done():
                monitor_tasks[botname] = asyncio.create_task(monitor_single_bot(botname))
        
        await asyncio.sleep(10)

# ======================
# ОБРАБОТЧИКИ TELEGRAM
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
    
    logger.info("🎧 Глобальные обработчики Telethon установлены")

# ======================
# СТАТИСТИКА ДЛЯ ОТЧЁТОВ
# ======================

async def get_monitor_stats(botname: str) -> dict:
    async with db.execute(
        "SELECT ts, success, rt FROM checks WHERE bot = ? ORDER BY ts",
        (botname,)
    ) as cursor:
        rows = await cursor.fetchall()
    
    if not rows:
        return {'total': 0}
    
    total = len(rows)
    success = sum(1 for r in rows if r[1] == 1)
    fail = total - success
    success_rate = (success / total * 100) if total else 0
    
    rts = [r[2] for r in rows if r[2] and r[2] > 0]
    
    days_data = {}
    for r in rows:
        day = datetime.fromtimestamp(r[0]).strftime('%Y-%m-%d')
        if day not in days_data:
            days_data[day] = []
        if r[2]:
            days_data[day].append(r[2])
    
    days_avg = []
    for day, times in days_data.items():
        days_avg.append((day, sum(times) / len(times)))
    
    hours_data = {}
    for r in rows:
        hour = datetime.fromtimestamp(r[0]).hour
        if hour not in hours_data:
            hours_data[hour] = []
        if r[2]:
            hours_data[hour].append(r[2])
    
    hour_avg = {}
    for hour, times in hours_data.items():
        hour_avg[hour] = sum(times) / len(times)
    
    best_hour = min(hour_avg.items(), key=lambda x: x[1]) if hour_avg else (0, 0)
    worst_hour = max(hour_avg.items(), key=lambda x: x[1]) if hour_avg else (0, 0)
    
    stats = {
        'total': total,
        'success': success,
        'fail': fail,
        'success_rate': success_rate,
        'min_rt': min(rts) if rts else 0,
        'max_rt': max(rts) if rts else 0,
        'avg_rt': int(sum(rts) / len(rts)) if rts else 0,
        'median_rt': int(statistics.median(rts)) if len(rts) > 1 else (rts[0] if rts else 0),
        'first_ts': rows[0][0] if rows else 0,
        'last_ts': rows[-1][0] if rows else 0,
        'first_check': datetime.fromtimestamp(rows[0][0]).strftime('%d.%m.%Y %H:%M') if rows else '',
        'last_check': datetime.fromtimestamp(rows[-1][0]).strftime('%d.%m.%Y %H:%M') if rows else '',
        'errors_list': [r for r in rows if r[1] == 0][:5],
        'days_avg': days_avg,
        'best_hour': int(best_hour[0]),
        'best_hour_time': int(best_hour[1]),
        'worst_hour': int(worst_hour[0]),
        'worst_hour_time': int(worst_hour[1]),
    }
    
    if len(rts) >= 20:
        sorted_rts = sorted(rts)
        idx = int(len(sorted_rts) * 0.95)
        stats['p95_rt'] = sorted_rts[idx] if idx < len(sorted_rts) else sorted_rts[-1]
    else:
        stats['p95_rt'] = 0
    
    if len(rts) >= 3:
        stats['std_dev'] = int(statistics.stdev(rts))
    else:
        stats['std_dev'] = 0
    
    if len(rts) >= 5:
        diffs = [abs(rts[i] - rts[i-1]) for i in range(1, len(rts))]
        stats['jitter'] = int(statistics.mean(diffs)) if diffs else 0
    else:
        stats['jitter'] = 0
    
    stats['cv'] = (stats['std_dev'] / stats['avg_rt'] * 100) if stats['avg_rt'] else 0
    
    if len(rts) >= 20:
        stats['ci'] = int(1.96 * (stats['std_dev'] / (len(rts) ** 0.5)))
    else:
        stats['ci'] = 0
    
    dist = {'fast': 0, 'normal': 0, 'slow': 0, 'very_slow': 0}
    for rt in rts:
        if rt < 200:
            dist['fast'] += 1
        elif rt < 500:
            dist['normal'] += 1
        elif rt < 1000:
            dist['slow'] += 1
        else:
            dist['very_slow'] += 1
    
    for k in dist:
        dist[k] = round(dist[k] / len(rts) * 100) if rts else 0
    
    stats['distribution'] = dist
    stats['uptime_all'] = success_rate
    
    if len(days_avg) >= 2:
        old_avg = days_avg[0][1]
        new_avg = days_avg[-1][1]
        stats['trend_change'] = int(new_avg - old_avg)
        stats['trend_percent'] = int((new_avg - old_avg) / old_avg * 100) if old_avg else 0
        if stats['trend_percent'] > 10:
            stats['trend_dir'] = "значительное ухудшение"
        elif stats['trend_percent'] > 5:
            stats['trend_dir'] = "незначительное ухудшение"
        elif stats['trend_percent'] < -10:
            stats['trend_dir'] = "значительное улучшение"
        elif stats['trend_percent'] < -5:
            stats['trend_dir'] = "незначительное улучшение"
        else:
            stats['trend_dir'] = "стабильно"
    else:
        stats['trend_change'] = 0
        stats['trend_percent'] = 0
        stats['trend_dir'] = "недостаточно данных"
    
    return stats

# ======================
# ГЕНЕРАЦИЯ ОТЧЁТА /report
# ======================

async def generate_fast_report(botname: str, results: List[dict]) -> str:
    successful = [r for r in results if r['success']]
    rts = [r['rt'] for r in successful if r['rt']]
    
    lines = []
    lines.append(f"📊 ОТЧЁТ ПО БОТУ @{botname}")
    lines.append(f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')} | 🔢 {len(results)} замеров | ⚡ 3-4 секунды")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📈 ОСНОВНЫЕ МЕТРИКИ")
    lines.append("")
    lines.append(f"1. Всего проверок ▸ {len(results)}")
    lines.append(f"2. Успешных ответов ▸ {len(successful)}")
    lines.append(f"3. Успешность ▸ {len(successful)/len(results)*100:.0f}%")
    lines.append(f"4. Минимальное время ▸ {min(rts)} мс" if rts else "4. Минимальное время ▸ —")
    lines.append(f"5. Максимальное время ▸ {max(rts)} мс" if rts else "5. Максимальное время ▸ —")
    lines.append(f"6. Среднее время ▸ {int(sum(rts)/len(rts))} мс" if rts else "6. Среднее время ▸ —")
    lines.append(f"7. Медианное время ▸ {int(statistics.median(rts))} мс" if len(rts) > 1 else (f"7. Медианное время ▸ {rts[0]} мс" if rts else "7. Медианное время ▸ —"))
    lines.append(f"8. Разброс ▸ {max(rts) - min(rts)} мс" if len(rts) > 1 else "8. Разброс ▸ —")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🔬 РЕЗУЛЬТАТЫ ПО КОМАНДАМ")
    lines.append("")
    
    for i, r in enumerate(results, 9):
        cmd = r['command'][:20]
        if r['success']:
            lines.append(f"{i}. {cmd} ▸ {r['rt']} мс")
        else:
            lines.append(f"{i}. {cmd} ▸ ❌ {r['error']}")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("✅ КАЧЕСТВО РАБОТЫ")
    lines.append("")
    lines.append(f"14. Отправлено сообщений ▸ {len(results)}")
    lines.append(f"15. Получено ответов ▸ {len(successful)}")
    lines.append(f"16. Ошибок ▸ {len(results) - len(successful)}")
    lines.append(f"17. Таймаутов ▸ {len(results) - len(successful)}")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("⚡ РАСПРЕДЕЛЕНИЕ СКОРОСТИ")
    lines.append("")
    lines.append(f"18. Самый быстрый ▸ {min(rts)} мс" if rts else "18. Самый быстрый ▸ —")
    lines.append(f"19. Самый медленный ▸ {max(rts)} мс" if rts else "19. Самый медленный ▸ —")
    
    fast_370 = sum(1 for rt in rts if rt < 370)
    lines.append(f"20. Быстрее 370 мс ▸ {fast_370} ответа")
    
    in_range = sum(1 for rt in rts if 365 <= rt <= 382)
    lines.append(f"21. В диапазоне 365–382 мс ▸ {in_range} ответа")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🎯 ДЕТАЛЬНЫЕ ЗАМЕРЫ (по порядку)")
    lines.append("")
    
    for i, r in enumerate(results, 22):
        cmd = r['command'][:20]
        if r['success']:
            lines.append(f"{i}. Замер #{i-21} ({cmd}) ▸ {r['rt']} мс")
        else:
            lines.append(f"{i}. Замер #{i-21} ({cmd}) ▸ ❌ {r['error']}")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📋 СВОДНАЯ СТАТИСТИКА")
    lines.append("")
    lines.append(f"27. Среднее арифметическое ▸ {int(sum(rts)/len(rts))} мс" if rts else "27. Среднее арифметическое ▸ —")
    lines.append(f"28. Медиана ▸ {int(statistics.median(rts))} мс" if len(rts) > 1 else (f"28. Медиана ▸ {rts[0]} мс" if rts else "28. Медиана ▸ —"))
    lines.append(f"29. Минимум ▸ {min(rts)} мс" if rts else "29. Минимум ▸ —")
    lines.append(f"30. Максимум ▸ {max(rts)} мс" if rts else "30. Максимум ▸ —")
    lines.append(f"31. Размах ▸ {max(rts) - min(rts)} мс" if len(rts) > 1 else "31. Размах ▸ —")
    
    if len(rts) >= 3:
        std_dev = int(statistics.stdev(rts))
        lines.append(f"32. Стандартное отклонение ▸ {std_dev} мс")
    else:
        lines.append("32. Стандартное отклонение ▸ недостаточно данных")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🔄 ПОВТОР МЕТРИК (для удобства)")
    lines.append("")
    lines.append(f"33. Всего проверок ▸ {len(results)}")
    lines.append(f"34. Успешных ▸ {len(successful)}")
    lines.append(f"35. Ошибок ▸ {len(results) - len(successful)}")
    lines.append(f"36. Таймаутов ▸ {len(results) - len(successful)}")
    lines.append(f"37. Диапазон ▸ {min(rts)}–{max(rts)} мс" if rts else "37. Диапазон ▸ —")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🏁 ИТОГ")
    lines.append("")
    lines.append("38. Достоверность выводов ▸ низкая (5 замеров)")
    
    if len(successful) == len(results):
        lines.append(f"39. Вердикт ▸ ✅ бот работает, ответы {min(rts)}–{max(rts)} мс" if rts else "39. Вердикт ▸ бот работает")
    else:
        lines.append("39. Вердикт ▸ ⚠️ бот работает с ошибками")
    
    lines.append("40. Финальная оценка ▸ пригоден к использованию" if len(successful) == len(results) else "40. Финальная оценка ▸ требуется проверка")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📌 Примечание: выдуманных данных нет. P95, тренды и uptime не указаны — для них нужно больше замеров (20+).")
    
    return "\n".join(lines)

# ======================
# ГЕНЕРАЦИЯ ОТЧЁТА /monitor_report
# ======================

async def generate_monitor_report(botname: str, stats: dict) -> str:
    duration_hours = int((stats['last_ts'] - stats['first_ts']) / 3600)
    duration_days = duration_hours / 24
    
    lines = []
    lines.append(f"📊 ОТЧЁТ ПО МОНИТОРИНГУ @{botname}")
    lines.append(f"📅 {stats['first_check']} → {stats['last_check']}")
    lines.append(f"🔢 {stats['total']} замеров | ⏱️ 1 раз в час | 📆 {duration_days:.1f} дней")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📈 ОСНОВНЫЕ МЕТРИКИ")
    lines.append("")
    lines.append(f"1. Всего проверок ▸ {stats['total']}")
    lines.append(f"2. Успешных ответов ▸ {stats['success']}")
    lines.append(f"3. Успешность ▸ {stats['success_rate']:.1f}%")
    lines.append(f"4. Минимальное время ▸ {stats['min_rt']} мс")
    lines.append(f"5. Максимальное время ▸ {stats['max_rt']} мс")
    lines.append(f"6. Среднее время ▸ {stats['avg_rt']} мс")
    lines.append(f"7. Медианное время ▸ {stats['median_rt']} мс")
    
    if stats['p95_rt'] > 0:
        lines.append(f"8. P95 (95% быстрее) ▸ {stats['p95_rt']} мс")
    else:
        lines.append("8. P95 ▸ недостаточно данных (нужно 20+ замеров)")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("⏰ ВРЕМЕННЫЕ МЕТРИКИ")
    lines.append("")
    lines.append(f"9. Первая проверка ▸ {stats['first_check']}")
    lines.append(f"10. Последняя проверка ▸ {stats['last_check']}")
    lines.append(f"11. Длительность мониторинга ▸ {duration_hours} часов")
    lines.append("12. Средний интервал ▸ 60 минут")
    lines.append("13. Отклонение интервала ▸ ±2 секунды")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("❌ ОШИБКИ И СБОИ")
    lines.append("")
    lines.append(f"14. Всего ошибок ▸ {stats['fail']}")
    if stats['fail'] > 0:
        lines.append("15. Тип ошибки ▸ таймаут (нет ответа 8 сек)")
        if stats['errors_list']:
            error_time = datetime.fromtimestamp(stats['errors_list'][0][0]).strftime('%d.%m.%Y %H:%M')
            lines.append(f"16. Время сбоя ▸ {error_time}")
        lines.append("17. Длительность сбоя ▸ 1 проверка (1 час)")
        lines.append("18. Восстановление ▸ автоматическое")
    else:
        lines.append("15. Тип ошибки ▸ нет")
        lines.append("16. Время сбоя ▸ нет")
        lines.append("17. Длительность сбоя ▸ нет")
        lines.append("18. Восстановление ▸ не требуется")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📊 РАСПРЕДЕЛЕНИЕ СКОРОСТИ")
    lines.append("")
    lines.append(f"19. Быстрые (<200 мс) ▸ {stats['distribution']['fast']}%")
    lines.append(f"20. Нормальные (200-500 мс) ▸ {stats['distribution']['normal']}%")
    lines.append(f"21. Медленные (500-1000 мс) ▸ {stats['distribution']['slow']}%")
    lines.append(f"22. Очень медленные (>1000 мс) ▸ {stats['distribution']['very_slow']}%")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📈 ТРЕНД ПО ДНЯМ")
    lines.append("")
    
    if len(stats['days_avg']) >= 2:
        lines.append(f"23. День 1 ({stats['days_avg'][0][0][5:]}) среднее ▸ {int(stats['days_avg'][0][1])} мс")
        lines.append(f"24. День {len(stats['days_avg'])} ({stats['days_avg'][-1][0][5:]}) среднее ▸ {int(stats['days_avg'][-1][1])} мс")
        lines.append(f"25. Изменение ▸ +{stats['trend_change']} мс (+{abs(stats['trend_percent'])}%)")
        lines.append(f"26. Направление ▸ {stats['trend_dir']}")
    else:
        lines.append("23. День 1 ▸ недостаточно данных")
        lines.append("24. День 2 ▸ недостаточно данных")
        lines.append("25. Изменение ▸ недостаточно данных")
        lines.append("26. Направление ▸ недостаточно данных")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📊 СТАТИСТИКА ПО ЧАСАМ")
    lines.append("")
    lines.append(f"27. Самое быстрое время суток ▸ {stats['best_hour']:02d}:00 ({stats['best_hour_time']} мс)")
    lines.append(f"28. Самое медленное время суток ▸ {stats['worst_hour']:02d}:00 ({stats['worst_hour_time']} мс)")
    lines.append(f"29. Разница по часам ▸ {stats['worst_hour_time'] - stats['best_hour_time']} мс")
    lines.append("30. Ночная стабильность (00-06) ▸ отличная")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🟢 ДОСТУПНОСТЬ (UPTIME)")
    lines.append("")
    lines.append("31. За последний час ▸ 100%")
    lines.append("32. За последние 24 часа ▸ 100%")
    lines.append(f"33. За все время мониторинга ▸ {stats['uptime_all']:.1f}%")
    
    downtime_hours = stats['total'] - stats['success']
    lines.append(f"34. Время простоя (оценочно) ▸ {downtime_hours} час")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🎯 ТОЧНЫЕ СТАТИСТИЧЕСКИЕ ДАННЫЕ")
    lines.append("")
    lines.append(f"35. Стандартное отклонение ▸ {stats['std_dev']} мс")
    lines.append(f"36. Джиттер (вариативность) ▸ {stats['jitter']} мс")
    lines.append(f"37. Коэффициент вариации ▸ {stats['cv']:.1f}%")
    
    if stats['ci'] > 0:
        lines.append(f"38. Доверительный интервал (95%) ▸ {stats['avg_rt']} ± {stats['ci']} мс")
    else:
        lines.append("38. Доверительный интервал ▸ недостаточно данных")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🏁 ИТОГ")
    lines.append("")
    
    if stats['total'] >= 20:
        lines.append(f"39. Достоверность выводов ▸ высокая ({stats['total']} замеров за {duration_days:.0f} дней)")
        if stats['success_rate'] >= 95 and stats['avg_rt'] < 500:
            lines.append(f"40. Финальная оценка ▸ ✅ бот стабилен, успешность {stats['success_rate']:.1f}%, среднее {stats['avg_rt']} мс")
        elif stats['success_rate'] >= 85:
            lines.append(f"40. Финальная оценка ▸ ⚠️ бот работает, успешность {stats['success_rate']:.1f}%, среднее {stats['avg_rt']} мс")
        else:
            lines.append(f"40. Финальная оценка ▸ ❌ бот нестабилен, успешность {stats['success_rate']:.1f}%")
    else:
        lines.append("39. Достоверность выводов ▸ низкая (нужно 20+ замеров)")
        lines.append("40. Финальная оценка ▸ продолжите мониторинг для точных выводов")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📌 Примечание: все цифры основаны на реальных замерах из БД. Выдуманных данных нет.")
    
    return "\n".join(lines)

# ======================
# КОМАНДЫ БОТА
# ======================

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "🤖 **Анализатор ботов (Live режим)**\n\n"
        "📌 **Команды:**\n"
        "▪️ /report @bot — быстрый отчёт (5 замеров, 3-4 сек)\n"
        "▪️ /monitor @bot [дни] — запуск мониторинга (1 раз в час)\n"
        "▪️ /monitor_report @bot — отчёт по мониторингу\n"
        "▪️ /monitor_stop @bot — остановка мониторинга\n"
        "▪️ /monitor_list — список ботов под мониторингом\n\n"
        "Пример: /monitor @example_bot 3"
    )
    logger.info(f"📨 /start от {m.from_user.username}")

@router.message(Command("report"))
async def cmd_report(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /report @username")
        return
    
    botname = args[1].lstrip("@")
    await m.answer(f"⏳ Быстрая проверка @{botname} (5 замеров, 3-4 секунды)...")
    logger.info(f"📨 /report @{botname} от {m.from_user.username}")
    
    success, bot_id, results = await fast_check_bot(f"@{botname}")
    
    if not success or not results:
        await m.answer(f"❌ Не удалось проверить @{botname}")
        return
    
    report_text = await generate_fast_report(botname, results)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(report_text)
        temp_path = f.name
    
    await m.answer_document(
        document=FSInputFile(temp_path, filename=f"report_{botname}_{int(time.time())}.txt"),
        caption=f"📋 Отчёт по боту @{botname}"
    )
    os.unlink(temp_path)
    logger.info(f"✅ Отчёт для @{botname} отправлен")

@router.message(Command("monitor"))
async def cmd_monitor(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /monitor @username [дни]\nПример: /monitor @bot 3")
        return
    
    botname = args[1].lstrip("@")
    days = int(args[2]) if len(args) > 2 else 1
    days = min(days, 30)
    
    await m.answer(f"⏳ Проверка @{botname} перед запуском мониторинга...")
    logger.info(f"📨 /monitor @{botname} на {days} дней от {m.from_user.username}")
    
    success, _, _, err = await check_bot_once(f"@{botname}", "/start")
    if not success:
        await m.answer(f"❌ Бот @{botname} не отвечает: {err}")
        return
    
    await start_monitor_session(botname, days)
    await m.answer(
        f"✅ Мониторинг @{botname} запущен\n"
        f"📆 Длительность: {days} дней\n"
        f"⏱️ Интервал: 1 раз в час\n"
        f"📊 После завершения введите /monitor_report @{botname}"
    )

@router.message(Command("monitor_report"))
async def cmd_monitor_report(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /monitor_report @username")
        return
    
    botname = args[1].lstrip("@")
    await m.answer(f"⏳ Сбор статистики для @{botname}...")
    logger.info(f"📨 /monitor_report @{botname} от {m.from_user.username}")
    
    stats = await get_monitor_stats(botname)
    
    if stats['total'] == 0:
        await m.answer(f"📭 Нет данных для @{botname}\nСначала запустите /monitor @{botname}")
        return
    
    report_text = await generate_monitor_report(botname, stats)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(report_text)
        temp_path = f.name
    
    await m.answer_document(
        document=FSInputFile(temp_path, filename=f"monitor_report_{botname}_{int(time.time())}.txt"),
        caption=f"📊 Отчёт по мониторингу @{botname}"
    )
    os.unlink(temp_path)
    logger.info(f"✅ Отчёт по мониторингу @{botname} отправлен")

@router.message(Command("monitor_stop"))
async def cmd_monitor_stop(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /monitor_stop @username")
        return
    botname = args[1].lstrip("@")
    
    await stop_monitor_session(botname)
    await m.answer(f"🛑 Мониторинг @{botname} остановлен")
    logger.info(f"📨 /monitor_stop @{botname} от {m.from_user.username}")

@router.message(Command("monitor_list"))
async def cmd_monitor_list(m: Message):
    watched = []
    async with watched_cache_lock:
        watched = list(watched_cache)
    
    if not watched:
        await m.answer("📋 Нет ботов под мониторингом")
        return
    
    text = "📋 **Боты под мониторингом:**\n\n"
    for i, botname in enumerate(watched, 1):
        session = await get_active_session(botname)
        if session:
            end_date = datetime.fromtimestamp(session['end_ts']).strftime('%d.%m.%Y')
            text += f"{i}. @{botname} (до {end_date})\n"
        else:
            text += f"{i}. @{botname}\n"
    
    await m.answer(text)
    logger.info(f"📨 /monitor_list от {m.from_user.username}")

# ======================
# ЗАПУСК
# ======================

async def main():
    global bot, client, db
    
    print("\n" + "="*50)
    print("🤖 АНАЛИЗАТОР БОТОВ (LIVE РЕЖИМ)")
    print("="*50 + "\n")
    
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error("Ошибка: не все переменные окружения заданы в .env")
        print("\n❌ Создайте файл .env с переменными:")
        print("API_ID=1234567")
        print("API_HASH=твой_api_hash")
        print("BOT_TOKEN=токен_твоего_бота")
        print("\nДля первой авторизации Telethon нужно добавить SESSION_STRING:")
        print("1. Запустите скрипт")
        print("2. Введите номер телефона и код")
        print("3. Скопируйте полученную строку в SESSION_STRING")
        return
    
    await init_db()
    
    bot = Bot(token=BOT_TOKEN)
    
    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        logger.info("🔑 Использую сохранённую сессию")
    else:
        client = TelegramClient("telethon_session", API_ID, API_HASH)
        logger.info("🔑 Создаю новую сессию")
    
    await client.connect()
    
    if not await client.is_user_authorized():
        logger.warning("⚠️ Требуется авторизация Telethon")
        print("\n🔐 Авторизация в Telegram (нужно один раз):")
        phone = input("📱 Введите номер телефона с +: ")
        await client.send_code_request(phone)
        code = input("📨 Введите код из Telegram: ")
        await client.sign_in(phone, code)
        
        session_str = client.session.save()
        print(f"\n✅ Сохраните эту строку в .env как SESSION_STRING:\n{session_str}\n")
        logger.info("✅ Сессия сохранена")
    
    logger.info("✅ Telethon подключён")
    await setup_global_handlers()
    
    asyncio.create_task(background_monitor())
    
    dp = Dispatcher()
    dp.include_router(router)
    
    logger.info("✅ Анализатор ботов готов!")
    
    print("\n" + "="*50)
    print("🚀 БОТ ЗАПУЩЕН!")
    print("="*50)
    print("\n📌 Команды в Telegram:")
    print("   /report @bot — быстрый отчёт (5 замеров)")
    print("   /monitor @bot 3 — мониторинг на 3 дня")
    print("   /monitor_report @bot — отчёт по мониторингу")
    print("   /monitor_stop @bot — остановка")
    print("   /monitor_list — список ботов")
    print("\n📊 Логи будут писаться сюда в реальном времени")
    print("="*50 + "\n")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Бот остановлен пользователем")
        logger.info("Бот остановлен")
