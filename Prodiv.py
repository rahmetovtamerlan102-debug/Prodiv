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
MONITOR_INTERVAL = 3600  # 1 час
PORT = int(os.getenv("PORT", "8080"))

# Настройка красивых логов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
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
# HEALTH CHECK ДЛЯ RENDER
# ======================

async def health_check(request):
    """Проверка здоровья для Render"""
    return web.Response(text="OK", status=200)

async def start_health_server():
    """Запускает HTTP сервер для health check"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"✅ Health check сервер запущен на порту {PORT}")

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

async def remove_from_monitor(botname: str):
    async with watched_cache_lock:
        watched_cache.discard(botname)

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
            await asyncio.wait_for(queue.get(), timeout=RESPONSE_TIMEOUT)
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
    """5 быстрых проверок для /report"""
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
        
        logger.info(f"🔍 Быстрая проверка @{username} (5 замеров)...")
        
        for i, cmd in enumerate(commands, 1):
            queue = asyncio.Queue()
            pending_checks[bot_id] = queue
            
            start = time.perf_counter()
            await client.send_message(entity, cmd)
            
            try:
                await asyncio.wait_for(queue.get(), timeout=RESPONSE_TIMEOUT)
                rt = int((time.perf_counter() - start) * 1000)
                results.append({'command': cmd, 'success': True, 'rt': rt, 'error': ''})
                logger.info(f"   Замер {i}/5: {rt} мс ✅")
            except asyncio.TimeoutError:
                results.append({'command': cmd, 'success': False, 'rt': None, 'error': 'Нет ответа'})
                logger.info(f"   Замер {i}/5: ❌ Нет ответа")
            finally:
                pending_checks.pop(bot_id, None)
            
            await asyncio.sleep(0.3)
        
        await save_fast_check(username[1:] if username.startswith('@') else username, bot_id, results)
        return True, bot_id, results
        
    except Exception as e:
        logger.error(f"❌ Ошибка быстрой проверки: {e}")
        return False, 0, []

# ======================
# ФОНОВЫЙ МОНИТОРИНГ
# ======================

async def monitor_single_bot(botname: str):
    while not shutdown_event.is_set():
        try:
            session = await get_active_session(botname)
            if not session:
                break
            
            if session['end_ts'] < int(time.time()):
                await stop_monitor_session(botname)
                break
            
            ok, bot_id, rt, err = await check_bot_once(f"@{botname}", "/start")
            await save_check(botname, bot_id, ok, rt, err, "/start")
            
            if ok:
                logger.info(f"📊 @{botname} → {rt} мс ✅")
            else:
                logger.warning(f"📊 @{botname} → {err} ❌")
                
        except Exception as e:
            logger.error(f"❌ Ошибка мониторинга @{botname}: {e}")
        
        await asyncio.sleep(MONITOR_INTERVAL)

async def background_monitor():
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
    
    logger.info("🎧 Глобальные обработчики установлены")

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
    
    return stats

# ======================
# ГЕНЕРАЦИЯ ОТЧЁТА /report (честный, только цифры)
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
    
    if rts:
        lines.append(f"4. Минимальное время ▸ {min(rts)} мс")
        lines.append(f"5. Максимальное время ▸ {max(rts)} мс")
        lines.append(f"6. Среднее время ▸ {int(sum(rts)/len(rts))} мс")
        if len(rts) > 1:
            lines.append(f"7. Медианное время ▸ {int(statistics.median(rts))} мс")
            lines.append(f"8. Разброс ▸ {max(rts) - min(rts)} мс")
        else:
            lines.append(f"7. Медианное время ▸ {rts[0]} мс")
            lines.append("8. Разброс ▸ —")
    else:
        lines.append("4. Минимальное время ▸ —")
        lines.append("5. Максимальное время ▸ —")
        lines.append("6. Среднее время ▸ —")
        lines.append("7. Медианное время ▸ —")
        lines.append("8. Разброс ▸ —")
    
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
    
    if rts:
        lines.append(f"18. Самый быстрый ▸ {min(rts)} мс")
        lines.append(f"19. Самый медленный ▸ {max(rts)} мс")
        fast_370 = sum(1 for rt in rts if rt < 370)
        lines.append(f"20. Быстрее 370 мс ▸ {fast_370} ответа")
        in_range = sum(1 for rt in rts if 365 <= rt <= 382)
        lines.append(f"21. В диапазоне 365–382 мс ▸ {in_range} ответа")
    else:
        lines.append("18. Самый быстрый ▸ —")
        lines.append("19. Самый медленный ▸ —")
        lines.append("20. Быстрее 370 мс ▸ —")
        lines.append("21. В диапазоне 365–382 мс ▸ —")
    
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
    
    if rts:
        lines.append(f"27. Среднее арифметическое ▸ {int(sum(rts)/len(rts))} мс")
        if len(rts) > 1:
            lines.append(f"28. Медиана ▸ {int(statistics.median(rts))} мс")
        else:
            lines.append(f"28. Медиана ▸ {rts[0]} мс")
        lines.append(f"29. Минимум ▸ {min(rts)} мс")
        lines.append(f"30. Максимум ▸ {max(rts)} мс")
        if len(rts) > 1:
            lines.append(f"31. Размах ▸ {max(rts) - min(rts)} мс")
        else:
            lines.append("31. Размах ▸ —")
        if len(rts) >= 3:
            std_dev = int(statistics.stdev(rts))
            lines.append(f"32. Стандартное отклонение ▸ {std_dev} мс")
        else:
            lines.append("32. Стандартное отклонение ▸ недостаточно данных (нужно 3+ замера)")
    else:
        lines.append("27. Среднее арифметическое ▸ —")
        lines.append("28. Медиана ▸ —")
        lines.append("29. Минимум ▸ —")
        lines.append("30. Максимум ▸ —")
        lines.append("31. Размах ▸ —")
        lines.append("32. Стандартное отклонение ▸ —")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🔄 ПОВТОР МЕТРИК (для удобства)")
    lines.append("")
    lines.append(f"33. Всего проверок ▸ {len(results)}")
    lines.append(f"34. Успешных ▸ {len(successful)}")
    lines.append(f"35. Ошибок ▸ {len(results) - len(successful)}")
    lines.append(f"36. Таймаутов ▸ {len(results) - len(successful)}")
    if rts:
        lines.append(f"37. Диапазон ▸ {min(rts)}–{max(rts)} мс")
    else:
        lines.append("37. Диапазон ▸ —")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🏁 ИТОГ")
    lines.append("")
    lines.append("38. Достоверность выводов ▸ низкая (5 замеров)")
    
    if len(successful) == len(results) and rts:
        lines.append(f"39. Вердикт ▸ ✅ бот работает, ответы {min(rts)}–{max(rts)} мс")
        lines.append("40. Финальная оценка ▸ пригоден к использованию")
    elif len(successful) > 0:
        lines.append("39. Вердикт ▸ ⚠️ бот работает с ошибками")
        lines.append("40. Финальная оценка ▸ требуется проверка")
    else:
        lines.append("39. Вердикт ▸ ❌ бот не отвечает")
        lines.append("40. Финальная оценка ▸ не пригоден к использованию")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📌 Примечание: выдуманных данных нет. P95, тренды и uptime не указаны — для них нужно больше замеров (20+).")
    
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

@router.message(Command("report"))
async def cmd_report(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /report @username")
        return
    
    botname = args[1].lstrip("@")
    await m.answer(f"⏳ Быстрая проверка @{botname} (5 замеров, 3-4 секунды)...")
    
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
    
    stats = await get_monitor_stats(botname)
    
    if stats['total'] == 0:
        await m.answer(f"📭 Нет данных для @{botname}\nСначала запустите /monitor @{botname}")
        return
    
    # Простой отчёт по мониторингу
    lines = []
    lines.append(f"📊 ОТЧЁТ ПО МОНИТОРИНГУ @{botname}")
    lines.append(f"📅 {stats['first_check']} → {stats['last_check']}")
    lines.append(f"🔢 {stats['total']} замеров | ⏱️ 1 раз в час")
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
    lines.append("❌ ОШИБКИ И СБОИ")
    lines.append("")
    lines.append(f"9. Всего ошибок ▸ {stats['fail']}")
    lines.append(f"10. Успешность ▸ {stats['success_rate']:.1f}%")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("🏁 ИТОГ")
    lines.append("")
    
    if stats['total'] >= 20:
        lines.append(f"11. Достоверность выводов ▸ высокая ({stats['total']} замеров)")
        if stats['success_rate'] >= 95 and stats['avg_rt'] < 500:
            lines.append(f"12. Финальная оценка ▸ ✅ бот стабилен, успешность {stats['success_rate']:.1f}%, среднее {stats['avg_rt']} мс")
        elif stats['success_rate'] >= 85:
            lines.append(f"12. Финальная оценка ▸ ⚠️ бот работает, успешность {stats['success_rate']:.1f}%, среднее {stats['avg_rt']} мс")
        else:
            lines.append(f"12. Финальная оценка ▸ ❌ бот нестабилен, успешность {stats['success_rate']:.1f}%")
    else:
        lines.append("11. Достоверность выводов ▸ низкая (нужно 20+ замеров)")
        lines.append("12. Финальная оценка ▸ продолжите мониторинг для точных выводов")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("📌 Примечание: все цифры основаны на реальных замерах из БД. Выдуманных данных нет.")
    
    report_text = "\n".join(lines)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(report_text)
        temp_path = f.name
    
    await m.answer_document(
        document=FSInputFile(temp_path, filename=f"monitor_report_{botname}_{int(time.time())}.txt"),
        caption=f"📊 Отчёт по мониторингу @{botname}"
    )
    os.unlink(temp_path)

@router.message(Command("monitor_stop"))
async def cmd_monitor_stop(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /monitor_stop @username")
        return
    botname = args[1].lstrip("@")
    
    await stop_monitor_session(botname)
    await m.answer(f"🛑 Мониторинг @{botname} остановлен")

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

# ======================
# ЗАПУСК
# ======================

async def main():
    global bot, client, db
    
    print("\n" + "="*50)
    print("🤖 АНАЛИЗАТОР БОТОВ")
    print("="*50 + "\n")
    
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error("❌ Не все переменные окружения заданы в .env")
        print("\n❌ Создайте файл .env с переменными:")
        print("API_ID=1234567")
        print("API_HASH=твой_api_hash")
        print("BOT_TOKEN=токен_твоего_бота")
        return
    
    await init_db()
    
    # Запускаем health check сервер для Render
    await start_health_server()
    
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
    print("\n📊 Health check: http://localhost:{}/health".format(PORT))
    print("="*50 + "\n")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Бот остановлен")
        logger.info("Бот остановлен")
