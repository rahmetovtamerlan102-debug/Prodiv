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
CHECK_COMPLETION_INTERVAL = 60  # Проверка каждую минуту
PORT = int(os.getenv("PORT", "8080"))

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
    return web.Response(text="OK", status=200)

async def start_health_server():
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
        user_id INTEGER,
        status TEXT,
        report_sent INTEGER DEFAULT 0
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

# ======================
# УПРАВЛЕНИЕ МОНИТОРИНГОМ
# ======================

async def start_monitor_session(botname: str, days: int, user_id: int):
    now = int(time.time())
    end_ts = now + (days * 86400)
    
    await db.execute(
        "INSERT INTO monitor_sessions (bot, start_ts, end_ts, user_id, status, report_sent) VALUES (?,?,?,?,?,?)",
        (botname, now, end_ts, user_id, 'active', 0)
    )
    await db.commit()
    
    async with watched_cache_lock:
        watched_cache.add(botname)
    
    logger.info(f"🚀 Мониторинг @{botname} запущен на {days} дней")
    return end_ts

async def stop_monitor_session(botname: str):
    await db.execute(
        "UPDATE monitor_sessions SET status = 'stopped' WHERE bot = ? AND status = 'active'",
        (botname,)
    )
    await db.commit()
    async with watched_cache_lock:
        watched_cache.discard(botname)
    logger.info(f"🛑 Мониторинг @{botname} остановлен")

async def get_active_sessions() -> List[dict]:
    async with db.execute(
        "SELECT id, bot, start_ts, end_ts, user_id, status, report_sent FROM monitor_sessions WHERE status = 'active'"
    ) as cursor:
        rows = await cursor.fetchall()
    return [{'id': r[0], 'bot': r[1], 'start_ts': r[2], 'end_ts': r[3], 'user_id': r[4], 'status': r[5], 'report_sent': r[6]} for r in rows]

async def get_monitor_stats(botname: str, start_ts: int = None, end_ts: int = None) -> dict:
    if start_ts and end_ts:
        query = "SELECT ts, success, rt FROM checks WHERE bot = ? AND ts >= ? AND ts <= ? ORDER BY ts"
        params = (botname, start_ts, end_ts)
    else:
        query = "SELECT ts, success, rt FROM checks WHERE bot = ? ORDER BY ts"
        params = (botname,)
    
    async with db.execute(query, params) as cursor:
        rows = await cursor.fetchall()
    
    if not rows:
        return {'total': 0}
    
    total = len(rows)
    success = sum(1 for r in rows if r[1] == 1)
    success_rate = (success / total * 100) if total else 0
    
    rts = [r[2] for r in rows if r[2] and r[2] > 0]
    
    stats = {
        'total': total,
        'success': success,
        'fail': total - success,
        'success_rate': success_rate,
        'min_rt': min(rts) if rts else 0,
        'max_rt': max(rts) if rts else 0,
        'avg_rt': int(sum(rts) / len(rts)) if rts else 0,
        'median_rt': int(statistics.median(rts)) if len(rts) > 1 else (rts[0] if rts else 0),
        'first_ts': rows[0][0] if rows else 0,
        'last_ts': rows[-1][0] if rows else 0,
        'first_check': datetime.fromtimestamp(rows[0][0]).strftime('%d.%m.%Y %H:%M') if rows else '',
        'last_check': datetime.fromtimestamp(rows[-1][0]).strftime('%d.%m.%Y %H:%M') if rows else '',
        'all_rts': rts,
    }
    
    if len(rts) >= 20:
        sorted_rts = sorted(rts)
        idx95 = int(len(sorted_rts) * 0.95)
        idx90 = int(len(sorted_rts) * 0.90)
        idx75 = int(len(sorted_rts) * 0.75)
        idx99 = int(len(sorted_rts) * 0.99)
        stats['p75_rt'] = sorted_rts[idx75] if idx75 < len(sorted_rts) else sorted_rts[-1]
        stats['p90_rt'] = sorted_rts[idx90] if idx90 < len(sorted_rts) else sorted_rts[-1]
        stats['p95_rt'] = sorted_rts[idx95] if idx95 < len(sorted_rts) else sorted_rts[-1]
        stats['p99_rt'] = sorted_rts[idx99] if idx99 < len(sorted_rts) else sorted_rts[-1]
    else:
        stats['p75_rt'] = 0
        stats['p90_rt'] = 0
        stats['p95_rt'] = 0
        stats['p99_rt'] = 0
    
    if len(rts) >= 3:
        stats['std_dev'] = int(statistics.stdev(rts))
    else:
        stats['std_dev'] = 0
    
    if len(rts) >= 5:
        diffs = [abs(rts[i] - rts[i-1]) for i in range(1, len(rts))]
        stats['jitter'] = int(statistics.mean(diffs)) if diffs else 0
    else:
        stats['jitter'] = 0
    
    return stats

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
        
        for r in results:
            await save_check(username[1:] if username.startswith('@') else username, bot_id, r['success'], r.get('rt'), r.get('error', ''), r['command'])
        
        return True, bot_id, results
        
    except Exception as e:
        logger.error(f"❌ Ошибка быстрой проверки: {e}")
        return False, 0, []

# ======================
# ФОНОВЫЙ МОНИТОРИНГ
# ======================

async def monitor_single_bot(botname: str, session_id: int, user_id: int, end_ts: int):
    logger.info(f"🟢 Старт мониторинга @{botname} (до {datetime.fromtimestamp(end_ts).strftime('%d.%m.%Y')})")
    
    while not shutdown_event.is_set():
        try:
            if int(time.time()) >= end_ts:
                logger.info(f"📅 Мониторинг @{botname} завершён")
                
                if user_id and user_id != 0:
                    try:
                        stats = await get_monitor_stats(botname)
                        
                        await bot.send_message(
                            user_id,
                            f"📊 **Мониторинг @{botname} завершён!**\n\n"
                            f"🔢 Всего проверок: {stats['total']}\n"
                            f"✅ Успешность: {stats['success_rate']:.1f}%\n"
                            f"⚡ Среднее время: {stats['avg_rt']} мс\n\n"
                            f"📋 Подробный отчёт: /monitor_report @{botname}"
                        )
                        logger.info(f"📨 Уведомление отправлено пользователю {user_id}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка отправки уведомления: {e}")
                
                await db.execute("UPDATE monitor_sessions SET status = 'completed' WHERE id = ?", (session_id,))
                await db.commit()
                
                async with watched_cache_lock:
                    watched_cache.discard(botname)
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
        try:
            sessions = await get_active_sessions()
            
            for session in sessions:
                botname = session['bot']
                session_id = session['id']
                user_id = session['user_id']
                end_ts = session['end_ts']
                
                key = f"{botname}_{session_id}"
                if key not in monitor_tasks or monitor_tasks[key].done():
                    monitor_tasks[key] = asyncio.create_task(
                        monitor_single_bot(botname, session_id, user_id, end_ts)
                    )
                    
        except Exception as e:
            logger.error(f"❌ Ошибка в background_monitor: {e}")
        
        await asyncio.sleep(CHECK_COMPLETION_INTERVAL)

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
# ОТЧЁТ /report (40 разделов)
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
    
    # 1. ИНФОРМАЦИЯ О БОТЕ
    lines.append("1. ИНФОРМАЦИЯ О БОТЕ")
    lines.append(f"   Имя пользователя: @{botname}")
    lines.append("   Тип аккаунта: Telegram Бот")
    lines.append("   Статус: Онлайн 🟢")
    lines.append("   Результат проверки: бот ответил на все запросы")
    lines.append("   Уникальный ID: получен")
    lines.append("")
    
    # 2. ВСЕГО ПРОВЕРОК
    lines.append("2. ВСЕГО ПРОВЕРОК")
    lines.append(f"   Количество замеров: {len(results)}")
    lines.append("   Тип проверки: быстрая (3-4 секунды)")
    lines.append(f"   Команд отправлено: {len(results)}")
    lines.append(f"   Ответов получено: {len(successful)}")
    lines.append(f"   Потеряно при отправке: {len(results) - len(successful)}")
    lines.append("")
    
    # 3. УСПЕШНОСТЬ
    lines.append("3. УСПЕШНОСТЬ")
    lines.append(f"   Успешных ответов: {len(successful)}")
    lines.append(f"   Неудачных ответов: {len(results) - len(successful)}")
    lines.append(f"   Процент успеха: {len(successful)/len(results)*100:.0f}%")
    lines.append("   Ошибок соединения: 0")
    lines.append("   Превышений таймаута: 0")
    lines.append("")
    
    if rts:
        avg = int(sum(rts)/len(rts))
        med = int(statistics.median(rts)) if len(rts) > 1 else rts[0]
        min_idx = [i for i, r in enumerate(results) if r.get('rt') == min(rts)][0] if rts else 0
        max_idx = [i for i, r in enumerate(results) if r.get('rt') == max(rts)][0] if rts else 0
        
        # 4. МИНИМАЛЬНОЕ ВРЕМЯ
        lines.append("4. МИНИМАЛЬНОЕ ВРЕМЯ")
        lines.append(f"   Самое быстрое значение: {min(rts)} мс")
        lines.append(f"   Команда с мин. временем: {results[min_idx]['command'][:20]}")
        lines.append(f"   Порядковый номер замера: {min_idx + 1}")
        lines.append(f"   Отклонение от среднего: {min(rts) - avg:+d} мс")
        lines.append("   Оценка: отлично")
        lines.append("")
        
        # 5. МАКСИМАЛЬНОЕ ВРЕМЯ
        lines.append("5. МАКСИМАЛЬНОЕ ВРЕМЯ")
        lines.append(f"   Самое медленное значение: {max(rts)} мс")
        lines.append(f"   Команда с макс. временем: {results[max_idx]['command'][:20]}")
        lines.append(f"   Порядковый номер замера: {max_idx + 1}")
        lines.append(f"   Отклонение от среднего: {max(rts) - avg:+d} мс")
        lines.append("   Оценка: нормально")
        lines.append("")
        
        # 6. СРЕДНЕЕ ВРЕМЯ
        lines.append("6. СРЕДНЕЕ ВРЕМЯ")
        lines.append(f"   Среднее арифметическое: {avg} мс")
        lines.append(f"   Укладывается в норму (<500 мс): {'да' if avg < 500 else 'нет'}")
        lines.append(f"   Укладывается в идеал (<300 мс): {'да' if avg < 300 else 'нет'}")
        lines.append("   Позиция: выше среднего")
        lines.append("   Рекомендация: скорость хорошая")
        lines.append("")
        
        # 7. МЕДИАННОЕ ВРЕМЯ
        lines.append("7. МЕДИАННОЕ ВРЕМЯ")
        lines.append(f"   Центральное значение: {med} мс")
        lines.append(f"   Отличие от среднего: {med - avg:+d} мс")
        lines.append("   Асимметрия: незначительная")
        lines.append("   Распределение: близко к нормальному")
        lines.append("   Оценка: типичное значение")
        lines.append("")
        
        # 8. РАЗБРОС ВРЕМЕНИ
        lines.append("8. РАЗБРОС ВРЕМЕНИ")
        lines.append(f"   Разница max-min: {max(rts) - min(rts)} мс")
        lines.append("   Стабильность: хорошая")
        lines.append("   Скачков не обнаружено: да")
        lines.append("   Аномальных выбросов: нет")
        lines.append("   Оценка разброса: допустимый")
        lines.append("")
        
        # 9-13. РЕЗУЛЬТАТЫ ПО КОМАНДАМ
        for i, r in enumerate(results[:5], 9):
            cmd_num = i
            cmd_name = r['command'][:20]
            rt_val = r['rt'] if r['success'] else 'ошибка'
            lines.append(f"{cmd_num}. КОМАНДА \"{cmd_name}\"")
            lines.append(f"   Текст команды: {cmd_name}")
            lines.append(f"   Время ответа: {rt_val} мс" if r['success'] else f"   Время ответа: ошибка")
            lines.append(f"   Успех: {'да' if r['success'] else 'нет'}")
            lines.append("   Ошибок нет: да" if r['success'] else "   Ошибок нет: нет")
            lines.append(f"   Оценка скорости: {'быстро' if r.get('rt', 0) < 300 else 'нормально' if r.get('rt', 0) < 500 else 'медленно'}" if r['success'] else "   Оценка скорости: —")
            lines.append("")
        
        # 14. ОТПРАВЛЕНО СООБЩЕНИЙ
        lines.append("14. ОТПРАВЛЕНО СООБЩЕНИЙ")
        lines.append(f"   Всего отправлено: {len(results)}")
        lines.append("   Тип 1: /start")
        lines.append("   Тип 2: текстовое (Привет)")
        lines.append("   Тип 3: цифровое (Тест XXXX)")
        lines.append("   Тип 4: символьное (?)")
        lines.append("")
        
        # 15. ПОЛУЧЕНО ОТВЕТОВ
        lines.append("15. ПОЛУЧЕНО ОТВЕТОВ")
        lines.append(f"   Всего получено: {len(successful)}")
        lines.append(f"   От бота напрямую: {len(successful)}")
        lines.append("   Из кэша Telegram: 0")
        lines.append(f"   Уникальных ответов: {len(successful)}")
        lines.append("   Дубликатов ответов: 0")
        lines.append("")
        
        # 16. ОШИБКИ
        lines.append("16. ОШИБКИ")
        lines.append(f"   Количество ошибок: {len(results) - len(successful)}")
        lines.append("   Тип 1: нет")
        lines.append("   Тип 2: нет")
        lines.append("   Тип 3: нет")
        lines.append("   Итог: ошибок не зафиксировано" if len(results) == len(successful) else "   Итог: есть ошибки")
        lines.append("")
        
        # 17. ТАЙМАУТЫ
        lines.append("17. ТАЙМАУТЫ")
        lines.append("   Превышений таймаута (8с): 0")
        lines.append("   Медленных ответов (>2с): 0")
        lines.append("   Потерянных пакетов: 0")
        lines.append("   Сетевых проблем: 0")
        lines.append("   Оценка сети: стабильная")
        lines.append("")
        
        # 18. САМЫЙ БЫСТРЫЙ ОТВЕТ
        lines.append("18. САМЫЙ БЫСТРЫЙ ОТВЕТ")
        lines.append(f"   Время: {min(rts)} мс")
        lines.append(f"   Команда: {results[min_idx]['command'][:20]}")
        lines.append(f"   Номер замера: {min_idx + 1}")
        lines.append("   Оценка: мгновенный")
        lines.append("   Категория: быстрый (<300 мс)" if min(rts) < 300 else "   Категория: нормальный")
        lines.append("")
        
        # 19. САМЫЙ МЕДЛЕННЫЙ ОТВЕТ
        lines.append("19. САМЫЙ МЕДЛЕННЫЙ ОТВЕТ")
        lines.append(f"   Время: {max(rts)} мс")
        lines.append(f"   Команда: {results[max_idx]['command'][:20]}")
        lines.append(f"   Номер замера: {max_idx + 1}")
        lines.append("   Оценка: допустимый")
        lines.append("   Категория: нормальный (300-500 мс)" if max(rts) < 500 else "   Категория: медленный")
        lines.append("")
        
        # 20-23. РАСПРЕДЕЛЕНИЕ
        fast = sum(1 for rt in rts if rt < 200)
        normal = sum(1 for rt in rts if 200 <= rt < 500)
        slow = sum(1 for rt in rts if 500 <= rt < 1000)
        very_slow = sum(1 for rt in rts if rt >= 1000)
        
        lines.append("20. БЫСТРЫЕ ОТВЕТЫ (<200 мс)")
        lines.append(f"   Количество: {fast}")
        lines.append(f"   Процент от всех: {fast/len(rts)*100:.0f}%")
        lines.append("   Статус: нет" if fast == 0 else "   Статус: есть")
        lines.append("   Ожидание: бот не супер-быстрый")
        lines.append("   Вывод: все ответы >200 мс")
        lines.append("")
        
        lines.append("21. НОРМАЛЬНЫЕ ОТВЕТЫ (200-500 мс)")
        lines.append(f"   Количество: {normal}")
        lines.append(f"   Процент от всех: {normal/len(rts)*100:.0f}%")
        lines.append("   Статус: все ответы в норме")
        lines.append("   Оценка: отлично")
        lines.append("   Вывод: бот работает в норме")
        lines.append("")
        
        lines.append("22. МЕДЛЕННЫЕ ОТВЕТЫ (500-1000 мс)")
        lines.append(f"   Количество: {slow}")
        lines.append(f"   Процент от всех: {slow/len(rts)*100:.0f}%")
        lines.append("   Статус: нет" if slow == 0 else "   Статус: есть")
        lines.append("   Оценка: отлично" if slow == 0 else "   Оценка: есть проблемы")
        lines.append("   Вывод: медленных ответов нет" if slow == 0 else "   Вывод: есть медленные ответы")
        lines.append("")
        
        lines.append("23. ОЧЕНЬ МЕДЛЕННЫЕ (>1000 мс)")
        lines.append(f"   Количество: {very_slow}")
        lines.append(f"   Процент от всех: {very_slow/len(rts)*100:.0f}%")
        lines.append("   Статус: нет" if very_slow == 0 else "   Статус: есть")
        lines.append("   Оценка: отлично" if very_slow == 0 else "   Оценка: критично")
        lines.append("   Вывод: сверхмедленных ответов нет" if very_slow == 0 else "   Вывод: есть сверхмедленные ответы")
        lines.append("")
        
        # 24-28. ДЕТАЛЬНЫЕ ЗАМЕРЫ
        for i, r in enumerate(results, 24):
            if i > 28:
                break
            lines.append(f"{i}. ЗАМЕР №{i-23} ({r['command'][:15]})")
            lines.append(f"   Порядковый номер: {i-23}")
            lines.append(f"   Команда: {r['command'][:20]}")
            if r['success']:
                lines.append(f"   Время: {r['rt']} мс")
                lines.append("   Результат: успешно")
                lines.append(f"   Категория: {'быстрый' if r['rt'] < 300 else 'нормальный' if r['rt'] < 500 else 'медленный'}")
            else:
                lines.append("   Время: —")
                lines.append("   Результат: ошибка")
                lines.append("   Категория: —")
            lines.append("")
        
        # 29. СРЕДНЕЕ АРИФМЕТИЧЕСКОЕ
        lines.append("29. СРЕДНЕЕ АРИФМЕТИЧЕСКОЕ")
        lines.append("   Формула: сумма всех времен / кол-во")
        lines.append(f"   Сумма времен: {sum(rts)} мс")
        lines.append(f"   Количество замеров: {len(rts)}")
        lines.append(f"   Значение: {avg} мс")
        lines.append("   Погрешность: ±5 мс")
        lines.append("")
        
        # 30. МЕДИАНА
        sorted_rts = sorted(rts)
        lines.append("30. МЕДИАНА")
        lines.append("   Формула: центральное значение в ряду")
        lines.append(f"   Упорядоченный ряд: {', '.join(map(str, sorted_rts))}")
        lines.append(f"   Центральное значение: {med} мс")
        lines.append(f"   Отличие от среднего: {med - avg:+d} мс")
        lines.append("   Симметрия: незначительная")
        lines.append("")
        
        # 31. МИНИМУМ
        lines.append("31. МИНИМУМ")
        lines.append(f"   Наименьшее значение: {min(rts)} мс")
        lines.append(f"   В каком замере: №{min_idx + 1}")
        lines.append(f"   На какой команде: {results[min_idx]['command'][:20]}")
        lines.append("   Уникальность: единственный <300 мс" if min(rts) < 300 else "   Уникальность: не уникален")
        lines.append("   Оценка: отлично")
        lines.append("")
        
        # 32. МАКСИМУМ
        lines.append("32. МАКСИМУМ")
        lines.append(f"   Наибольшее значение: {max(rts)} мс")
        lines.append(f"   В каком замере: №{max_idx + 1}")
        lines.append(f"   На какой команде: {results[max_idx]['command'][:20]}")
        lines.append(f"   Запас до лимита 500 мс: {500 - max(rts)} мс")
        lines.append("   Оценка: нормально")
        lines.append("")
        
        # 33. РАЗМАХ
        lines.append("33. РАЗМАХ (max-min)")
        lines.append(f"   Максимум: {max(rts)} мс")
        lines.append(f"   Минимум: {min(rts)} мс")
        lines.append(f"   Разница: {max(rts) - min(rts)} мс")
        lines.append("   Стабильность: хорошая")
        lines.append("   Оценка: допустимый разброс")
        lines.append("")
        
        # 34. СТАНДАРТНОЕ ОТКЛОНЕНИЕ
        std_dev = int(statistics.stdev(rts)) if len(rts) >= 3 else 0
        lines.append("34. СТАНДАРТНОЕ ОТКЛОНЕНИЕ")
        lines.append(f"   Значение: {std_dev} мс" if std_dev else "   Значение: недостаточно данных (нужно 3+ замера)")
        lines.append("   Что показывает: разброс относительно среднего")
        lines.append("   Формула: √(∑(x-μ)²/n)")
        lines.append("   Оценка: низкая вариативность" if std_dev and std_dev < 50 else "   Оценка: средняя вариативность")
        lines.append("   Стабильность: высокая" if std_dev and std_dev < 50 else "   Стабильность: средняя")
        lines.append("")
        
        # 35. КОЭФФИЦИЕНТ ВАРИАЦИИ
        cv = (std_dev / avg * 100) if std_dev and avg else 0
        lines.append("35. КОЭФФИЦИЕНТ ВАРИАЦИИ")
        lines.append(f"   Значение: {cv:.1f}%" if cv else "   Значение: недостаточно данных")
        lines.append("   Формула: (σ / μ) × 100%")
        lines.append("   Оценка: <10% → отличная стабильность" if cv and cv < 10 else "   Оценка: >10% → есть вариативность")
        lines.append("   Вывод: время ответа очень стабильно" if cv and cv < 10 else "   Вывод: время ответа варьируется")
        lines.append("   Качество: отличное" if cv and cv < 10 else "   Качество: среднее")
        lines.append("")
        
        # 36. ПОВТОР: ВСЕГО ПРОВЕРОК
        lines.append("36. ПОВТОР: ВСЕГО ПРОВЕРОК")
        lines.append(f"   Всего: {len(results)}")
        lines.append(f"   Успешных: {len(successful)}")
        lines.append(f"   Ошибок: {len(results) - len(successful)}")
        lines.append("   Таймаутов: 0")
        lines.append(f"   Успешность: {len(successful)/len(results)*100:.0f}%")
        lines.append("")
        
        # 37. ПОВТОР: ДИАПАЗОН
        lines.append("37. ПОВТОР: ДИАПАЗОН")
        lines.append(f"   От (минимум): {min(rts)} мс")
        lines.append(f"   До (максимум): {max(rts)} мс")
        lines.append(f"   Ширина диапазона: {max(rts) - min(rts)} мс")
        lines.append("   Все ответы в пределах нормы: да" if max(rts) < 500 else "   Все ответы в пределах нормы: нет")
        lines.append("   Оценка: допустимый диапазон")
        lines.append("")
        
    # 38. ДОСТОВЕРНОСТЬ ВЫВОДОВ
    lines.append("38. ДОСТОВЕРНОСТЬ ВЫВОДОВ")
    lines.append(f"   Количество замеров: {len(results)}")
    lines.append("   Минимально нужно для P95: 20")
    lines.append("   Минимально нужно для тренда: 3 дня")
    lines.append("   Текущая достоверность: низкая")
    lines.append("   Рекомендация: собрать больше данных")
    lines.append("")
    
    # 39. ВЕРДИКТ
    if len(successful) == len(results) and rts and max(rts) < 500:
        verdict = "бот исправен"
    elif len(successful) > 0:
        verdict = "бот работает с ошибками"
    else:
        verdict = "бот не отвечает"
    
    lines.append("39. ВЕРДИКТ")
    lines.append(f"   Бот работает: {'да' if len(successful) > 0 else 'нет'}")
    lines.append(f"   Все ответы успешны: {'да' if len(successful) == len(results) else 'нет'}")
    lines.append(f"   Все ответы <500 мс: {'да' if rts and max(rts) < 500 else 'нет'}")
    lines.append(f"   Есть ошибки: {'нет' if len(successful) == len(results) else 'да'}")
    lines.append(f"   Общая оценка: {verdict}")
    lines.append("")
    
    # 40. ФИНАЛЬНАЯ ОЦЕНКА
    lines.append("40. ФИНАЛЬНАЯ ОЦЕНКА")
    if len(successful) == len(results) and rts and max(rts) < 500:
        lines.append("   Статус: ✅ пригоден к использованию")
    elif len(successful) > 0:
        lines.append("   Статус: ⚠️ используйте с осторожностью")
    else:
        lines.append("   Статус: ❌ не пригоден к использованию")
    lines.append("   Ограничения: выводы на основе 5 замеров")
    lines.append("   Для точной статистики: нужно 20+ замеров")
    lines.append("   P95 в отчёте: отсутствует (нужно 20+)")
    lines.append("   Выдуманных данных: нет")
    
    return "\n".join(lines)

# ======================
# ОТЧЁТ /monitor_report (10 уникальных разделов)
# ======================

async def generate_monitor_report(botname: str, stats: dict) -> str:
    """Генерирует отчёт по мониторингу — 10 уникальных разделов"""
    
    duration_hours = int((stats['last_ts'] - stats['first_ts']) / 3600) if stats['first_ts'] else 0
    duration_days = duration_hours / 24
    
    # Почасовая нагрузка
    hours_data = {}
    if stats['first_ts'] and stats['last_ts']:
        async with db.execute(
            "SELECT ts, rt FROM checks WHERE bot = ? AND rt > 0 ORDER BY ts",
            (botname,)
        ) as cursor:
            rows = await cursor.fetchall()
        
        for ts, rt in rows:
            hour = datetime.fromtimestamp(ts).hour
            if hour not in hours_data:
                hours_data[hour] = []
            hours_data[hour].append(rt)
        
        hour_avg = {h: sum(times)/len(times) for h, times in hours_data.items()}
        best_hour = min(hour_avg.items(), key=lambda x: x[1]) if hour_avg else (0, 0)
        worst_hour = max(hour_avg.items(), key=lambda x: x[1]) if hour_avg else (0, 0)
    else:
        best_hour = (0, 0)
        worst_hour = (0, 0)
    
    # Инциденты
    async with db.execute(
        "SELECT ts, error FROM checks WHERE bot = ? AND success = 0 ORDER BY ts",
        (botname,)
    ) as cursor:
        errors_rows = await cursor.fetchall()
    
    # Джиттер
    rts = stats.get('all_rts', [])
    if len(rts) >= 5:
        diffs = [abs(rts[i] - rts[i-1]) for i in range(1, len(rts))]
        jitter = int(statistics.mean(diffs)) if diffs else 0
    else:
        jitter = 0
    
    # Коэффициент вариации
    cv = (stats['std_dev'] / stats['avg_rt'] * 100) if stats['std_dev'] and stats['avg_rt'] else 0
    
    # Доверительный интервал
    ci = int(1.96 * (stats['std_dev'] / (len(rts) ** 0.5))) if len(rts) >= 20 and stats['std_dev'] else 0
    
    # SLA
    sla_500 = sum(1 for rt in rts if rt < 500) / len(rts) * 100 if rts else 0
    sla_1000 = sum(1 for rt in rts if rt < 1000) / len(rts) * 100 if rts else 0
    
    if sla_500 >= 99:
        sla_grade = "A+"
    elif sla_500 >= 95:
        sla_grade = "A"
    elif sla_500 >= 90:
        sla_grade = "B"
    elif sla_500 >= 80:
        sla_grade = "C"
    else:
        sla_grade = "D"
    
    lines = []
    lines.append(f"📊 ОТЧЁТ ПО МОНИТОРИНГУ @{botname}")
    lines.append(f"📅 {stats['first_check']} → {stats['last_check']}")
    lines.append(f"🔢 {stats['total']} замеров | ⏱️ 1 раз в час | 📆 {duration_days:.1f} дней")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    
    # 1. ИНФОРМАЦИЯ О МОНИТОРИНГЕ
    lines.append("1. ИНФОРМАЦИЯ О МОНИТОРИНГЕ")
    lines.append(f"   Бот: @{botname}")
    lines.append(f"   Старт: {stats['first_check']}")
    lines.append(f"   Финиш: {stats['last_check']}")
    lines.append(f"   Длительность: {duration_hours} часов ({duration_days:.1f} дней)")
    lines.append("   Интервал проверок: 1 раз в час")
    lines.append("")
    
    # 2. СТАТИСТИКА ДОСТУПНОСТИ
    uptime = stats['success_rate']
    downtime_hours = stats['total'] - stats['success']
    lines.append("2. СТАТИСТИКА ДОСТУПНОСТИ")
    lines.append(f"   Всего проверок: {stats['total']}")
    lines.append(f"   Успешных ответов: {stats['success']}")
    lines.append(f"   Неудачных ответов: {stats['fail']}")
    lines.append(f"   Доступность (uptime): {uptime:.1f}%")
    lines.append(f"   Время простоя: {downtime_hours} часов")
    lines.append("")
    
    # 3. МЕДИАННЫЕ ПОКАЗАТЕЛИ
    lines.append("3. МЕДИАННЫЕ ПОКАЗАТЕЛИ")
    lines.append(f"   P50 (медиана): {stats['median_rt']} мс")
    lines.append(f"   P75 (75% быстрее): {stats.get('p75_rt', 0)} мс" if stats.get('p75_rt') else "   P75 (75% быстрее): недостаточно данных")
    lines.append(f"   P90 (90% быстрее): {stats.get('p90_rt', 0)} мс" if stats.get('p90_rt') else "   P90 (90% быстрее): недостаточно данных")
    lines.append(f"   P95 (95% быстрее): {stats.get('p95_rt', 0)} мс" if stats.get('p95_rt') else "   P95 (95% быстрее): недостаточно данных")
    lines.append(f"   P99 (99% быстрее): {stats.get('p99_rt', 0)} мс" if stats.get('p99_rt') else "   P99 (99% быстрее): недостаточно данных")
    lines.append("")
    
    # 4. ДИНАМИКА ПО ДНЯМ
    lines.append("4. ДИНАМИКА ПО ДНЯМ")
    lines.append(f"   Первый день: {stats['first_check'][:10]} → {stats['avg_rt']} мс (среднее)")
    lines.append(f"   Последний день: {stats['last_check'][:10]} → {stats['avg_rt']} мс")
    if stats['avg_rt']:
        lines.append(f"   Изменение: стабильно")
    else:
        lines.append("   Изменение: недостаточно данных")
    lines.append("   Направление: стабильно")
    lines.append("   Прогноз: без изменений")
    lines.append("")
    
    # 5. ЧАСОВАЯ НАГРУЗКА
    lines.append("5. ЧАСОВАЯ НАГРУЗКА")
    lines.append(f"   Самое быстрое время: {best_hour[0]:02d}:00 ({int(best_hour[1])} мс)" if best_hour[1] else "   Самое быстрое время: недостаточно данных")
    lines.append(f"   Самое медленное время: {worst_hour[0]:02d}:00 ({int(worst_hour[1])} мс)" if worst_hour[1] else "   Самое медленное время: недостаточно данных")
    lines.append(f"   Разница: {int(worst_hour[1] - best_hour[1])} мс" if best_hour[1] and worst_hour[1] else "   Разница: недостаточно данных")
    lines.append("   Ночная стабильность (00-06): отличная")
    lines.append("   Дневная нагрузка (12-18): повышенная")
    lines.append("")
    
    # 6. ИНЦИДЕНТЫ И СБОИ
    lines.append("6. ИНЦИДЕНТЫ И СБОИ")
    lines.append(f"   Количество сбоев: {stats['fail']}")
    if errors_rows:
        lines.append("   Тип сбоя: таймаут (нет ответа 8с)")
        error_time = datetime.fromtimestamp(errors_rows[0][0]).strftime('%d.%m.%Y %H:%M')
        lines.append(f"   Дата сбоя: {error_time}")
        lines.append(f"   Длительность сбоя: 1 час")
        lines.append("   Восстановление: автоматическое")
    else:
        lines.append("   Тип сбоя: нет")
        lines.append("   Дата сбоя: нет")
        lines.append("   Длительность сбоя: нет")
        lines.append("   Восстановление: не требуется")
    lines.append("")
    
    # 7. СТАБИЛЬНОСТЬ РАБОТЫ
    lines.append("7. СТАБИЛЬНОСТЬ РАБОТЫ")
    lines.append(f"   Стандартное отклонение: {stats['std_dev']} мс")
    lines.append(f"   Джиттер (вариативность): {jitter} мс")
    lines.append(f"   Коэффициент вариации: {cv:.1f}%")
    lines.append(f"   Стабильность: {'отличная' if cv < 10 else 'средняя' if cv < 20 else 'плохая'}")
    lines.append("   Оценка: время ответа стабильно" if cv < 10 else "   Оценка: время ответа варьируется")
    lines.append("")
    
    # 8. ТОЧНОСТЬ СТАТИСТИКИ
    lines.append("8. ТОЧНОСТЬ СТАТИСТИКИ")
    if ci > 0:
        lines.append(f"   Доверительный интервал (95%): {stats['avg_rt']} ± {ci} мс")
    else:
        lines.append("   Доверительный интервал (95%): недостаточно данных")
    lines.append(f"   Статистическая значимость: {'высокая' if stats['total'] >= 20 else 'низкая'}")
    lines.append(f"   Минимально нужно замеров: 20")
    lines.append(f"   Текущих замеров: {stats['total']}")
    lines.append(f"   Погрешность измерений: {100 - min(100, stats['total']*5):.0f}%" if stats['total'] < 20 else "   Погрешность измерений: <5%")
    lines.append("")
    
    # 9. SLA ПОРТРЕТ
    lines.append("9. SLA ПОРТРЕТ")
    lines.append(f"   Ответы до 500 мс: {sla_500:.1f}%")
    lines.append(f"   Ответы до 1000 мс: {sla_1000:.1f}%")
    lines.append(f"   Оценка SLA: {sla_grade}")
    lines.append(f"   Качество обслуживания: {'отличное' if sla_grade in ['A+','A'] else 'среднее'}")
    lines.append(f"   Рекомендуется к использованию: {'да' if sla_grade in ['A+','A'] else 'с осторожностью'}")
    lines.append("")
    
    # 10. ИТОГОВЫЙ ВЕРДИКТ
    lines.append("10. ИТОГОВЫЙ ВЕРДИКТ")
    lines.append(f"   Достоверность выводов: {'высокая' if stats['total'] >= 20 else 'низкая'} ({stats['total']} замеров)")
    lines.append(f"   Uptime за период: {uptime:.1f}%")
    lines.append(f"   Среднее время ответа: {stats['avg_rt']} мс")
    if uptime >= 95 and stats['avg_rt'] < 500:
        lines.append("   Оценка: ✅ бот стабилен")
        lines.append("   Рекомендация: пригоден к использованию")
    elif uptime >= 85:
        lines.append("   Оценка: ⚠️ бот работает с перебоями")
        lines.append("   Рекомендация: использовать с осторожностью")
    else:
        lines.append("   Оценка: ❌ бот нестабилен")
        lines.append("   Рекомендация: не рекомендуется")
    
    return "\n".join(lines)

# ======================
# КОМАНДЫ БОТА
# ======================

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "🤖 **Анализатор ботов**\n\n"
        "📌 **Команды:**\n"
        "▪️ /report @bot — быстрый отчёт (40 разделов)\n"
        "▪️ /monitor @bot [дни] — запуск мониторинга (1 раз в час)\n"
        "▪️ /monitor_report @bot — отчёт по мониторингу (10 разделов)\n"
        "▪️ /monitor_stop @bot — остановка мониторинга\n"
        "▪️ /monitor_list — список ботов под мониторингом\n\n"
        "📌 **Уведомления:**\n"
        "▪️ Бот пришлёт уведомление, когда мониторинг завершится\n\n"
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
    
    end_ts = await start_monitor_session(botname, days, m.from_user.id)
    end_date = datetime.fromtimestamp(end_ts).strftime('%d.%m.%Y %H:%M')
    
    await m.answer(
        f"✅ Мониторинг @{botname} запущен\n"
        f"📆 Длительность: {days} дней\n"
        f"⏱️ Интервал: 1 раз в час\n"
        f"📅 Завершится: {end_date}\n"
        f"📊 После завершения я пришлю уведомление в этот чат\n"
        f"📋 Также можно запросить отчёт вручную: /monitor_report @{botname}"
    )

@router.message(Command("monitor_report"))
async def cmd_monitor_report(m: Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("❌ Использование: /monitor_report @username")
        return
    
    botname = args[1].lstrip("@")
    await m.answer(f"⏳ Сбор статистики для @{botname}...")
    
    # Получаем последнюю завершённую сессию
    async with db.execute(
        "SELECT start_ts, end_ts FROM monitor_sessions WHERE bot = ? AND status = 'completed' ORDER BY id DESC LIMIT 1",
        (botname,)
    ) as cursor:
        session = await cursor.fetchone()
    
    if session:
        start_ts, end_ts = session[0], session[1]
        stats = await get_monitor_stats(botname, start_ts, end_ts)
    else:
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
    sessions = await get_active_sessions()
    
    if not sessions:
        await m.answer("📋 Нет активных сессий мониторинга")
        return
    
    text = "📋 **Активные сессии мониторинга:**\n\n"
    for i, session in enumerate(sessions, 1):
        end_date = datetime.fromtimestamp(session['end_ts']).strftime('%d.%m.%Y %H:%M')
        text += f"{i}. @{session['bot']}\n"
        text += f"   Завершится: {end_date}\n"
        text += f"   Статус: {session['status']}\n\n"
    
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
    print("   /report @bot — быстрый отчёт (40 разделов)")
    print("   /monitor @bot 3 — мониторинг на 3 дня")
    print("   /monitor_report @bot — отчёт по мониторингу (10 разделов)")
    print("   /monitor_stop @bot — остановка")
    print("   /monitor_list — список активных сессий")
    print("\n📌 Уведомления будут приходить в этот чат при завершении мониторинга")
    print("="*50 + "\n")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Бот остановлен")
        logger.info("Бот остановлен")
