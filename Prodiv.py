#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import re
import logging
import zipfile
import io
from datetime import datetime
from typing import Optional, Dict, Any

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from bs4 import BeautifulSoup

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import UserStatusOnline, UserStatusOffline, UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth, UserStatusEmpty

# ---------- КОНФИГ ----------
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

BOT_LIU = "liuofxnhvm3dvqbot"
BOT_VEK = "vektokOsint_bot"
BOT_DATEREG = "dateregbot"
BOT_MORAX = "moraxgetbot"

TIMEOUT = 35
if not all([API_ID, API_HASH, SESSION_STRING, BOT_TOKEN]):
    raise Exception("Задайте API_ID, API_HASH, SESSION_STRING, BOT_TOKEN")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ------------------------------------------------------------
# 1. УНИВЕРСАЛЬНЫЙ ЗАПРОС (ТОЛЬКО ТЕКСТ, 1 РАЗ)
# ------------------------------------------------------------
async def ask_bot(bot_username: str, query: str, timeout: int = TIMEOUT) -> Optional[str]:
    try:
        entity = await user_client.get_entity(bot_username)
    except Exception as e:
        logger.error(f"Не найден {bot_username}: {e}")
        return None

    future = asyncio.get_event_loop().create_future()
    @user_client.on(events.NewMessage(from_users=entity.id))
    async def handler(event):
        if not future.done() and event.message.text:
            future.set_result(event.message.text)
            user_client.remove_event_handler(handler)

    await user_client.send_message(entity, query)
    try:
        return await asyncio.wait_for(future, timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут {bot_username}")
        return None
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass

# ------------------------------------------------------------
# 2. НОМЕР ТЕЛЕФОНА (liuofxnhvm3dvqbot)
# ------------------------------------------------------------
async def get_phone(username: str) -> Optional[str]:
    resp = await ask_bot(BOT_LIU, username, timeout=25)
    if resp:
        m = re.search(r'(\+?7\d{10})', resp)
        if m:
            phone = m.group(1)
            if not phone.startswith('+'):
                phone = '+' + phone
            return phone
    return None

# ------------------------------------------------------------
# 3. ДАННЫЕ ОТ VEKTOK (HTML/файл) – ТОЛЬКО ЕСЛИ ЕСТЬ НОМЕР
# ------------------------------------------------------------
async def get_vektok_data(phone: str) -> Dict[str, str]:
    result = {"operator": "", "region": "", "full_name": "", "bases_count": "", "records_count": "", "address": ""}
    if not phone:
        return result
    try:
        entity = await user_client.get_entity(BOT_VEK)
    except:
        return result

    future = asyncio.get_event_loop().create_future()
    @user_client.on(events.NewMessage(from_users=entity.id))
    @user_client.on(events.MessageEdited(from_users=entity.id))
    async def handler(event):
        msg = event.message
        if msg.document:
            file_path = await msg.download_media()
            try:
                with open(file_path, 'rb') as f:
                    data = f.read()
                if file_path.endswith('.zip'):
                    with zipfile.ZipFile(io.BytesIO(data)) as zf:
                        for name in zf.namelist():
                            if name.endswith('.html'):
                                html = zf.read(name).decode('utf-8', errors='ignore')
                                break
                else:
                    html = data.decode('utf-8', errors='ignore')
                parsed = parse_vektok_html(html)
                future.set_result(parsed)
            except Exception as e:
                logger.error(f"Vektok file error: {e}")
                future.set_result(None)
            finally:
                os.remove(file_path)
                user_client.remove_event_handler(handler)
        elif msg.text and ("не удалось" in msg.text.lower() or "не найдено" in msg.text.lower()):
            future.set_result(None)
            user_client.remove_event_handler(handler)

    await user_client.send_message(entity, phone)
    try:
        parsed = await asyncio.wait_for(future, timeout=35)
        if parsed:
            result.update(parsed)
    except asyncio.TimeoutError:
        logger.warning("Таймаут vektok")
    except Exception as e:
        logger.error(f"Vektok error: {e}")
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass
    return result

def parse_vektok_html(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator='\n')
    res = {}
    patterns = {
        "operator": r'Оператор:\s*(.+?)(?:\n|$)',
        "region": r'Регион:\s*(.+?)(?:\n|$)',
        "full_name": r'(?:Найденные данные:|ЗАПИСАН В БАЗАХ:)\s*(.+?)(?:\n|$)',
        "bases_count": r'Количество баз:\s*(\d+)',
        "records_count": r'Количество записей:\s*(\d+)',
        "address": r'Адрес:\s*(.+?)(?:\n|$)'
    }
    for k, p in patterns.items():
        m = re.search(p, text, re.I | re.M)
        if m:
            res[k] = m.group(1).strip()
    return res

# ------------------------------------------------------------
# 4. ДАТЫ ОТ DATEREGBOT
# ------------------------------------------------------------
async def get_dates(username: str) -> Dict[str, Optional[str]]:
    resp = await ask_bot(BOT_DATEREG, username, timeout=25)
    result = {"reg_date": None, "first_seen": None}
    if resp:
        m = re.search(r'Регистрация:\s*([^\n]+)', resp)
        if m:
            result["reg_date"] = m.group(1).strip()
        m = re.search(r'Появление:\s*([^\n]+)', resp)
        if m:
            result["first_seen"] = m.group(1).strip()
        if not result["reg_date"]:
            m = re.search(r'Account created:\s*([^\n]+)', resp, re.I)
            if m:
                result["reg_date"] = m.group(1).strip()
        if not result["first_seen"]:
            m = re.search(r'First seen:\s*([^\n]+)', resp, re.I)
            if m:
                result["first_seen"] = m.group(1).strip()
    return result

# ------------------------------------------------------------
# 5. ПРЯМОЙ API TELEGRAM (ID, СТАТУС, АВАТАР...)
# ------------------------------------------------------------
def get_status_text(status) -> str:
    if isinstance(status, UserStatusOnline):
        return "онлайн"
    elif isinstance(status, UserStatusOffline) and status.was_online:
        days = (datetime.now() - status.was_online).days
        if days == 0: return "сегодня"
        elif days < 7: return "на этой неделе"
        else: return "давно"
    elif isinstance(status, UserStatusRecently): return "недавно (до месяца)"
    elif isinstance(status, UserStatusLastWeek): return "на прошлой неделе"
    elif isinstance(status, UserStatusLastMonth): return "в прошлом месяце"
    elif isinstance(status, UserStatusEmpty): return "скрыто"
    return "неизвестно"

async def get_direct_info(identifier: str) -> Dict[str, Any]:
    res = {
        "id": None, "first_name": None, "last_name": None, "username": None,
        "usernames": [], "bio": None, "premium": False, "verified": False,
        "scam": False, "fake": False, "bot": False, "status_text": None,
        "reg_date": None, "has_avatar": False, "dc_id": None
    }
    try:
        entity = await user_client.get_entity(identifier)
        full = await user_client(GetFullUserRequest(entity.id))
        user = full.user
        res["id"] = user.id
        res["first_name"] = user.first_name
        res["last_name"] = user.last_name
        res["username"] = f"@{user.username}" if user.username else None
        if hasattr(user, 'usernames') and user.usernames:
            res["usernames"] = [f"@{u.username}" for u in user.usernames]
        res["bio"] = full.about
        res["premium"] = getattr(user, 'premium', False)
        res["verified"] = getattr(user, 'verified', False)
        res["scam"] = getattr(user, 'scam', False)
        res["fake"] = getattr(user, 'fake', False)
        res["bot"] = getattr(user, 'bot', False)
        res["status_text"] = get_status_text(user.status)
        if hasattr(user, 'date') and user.date:
            res["reg_date"] = user.date.strftime("%Y-%m-%d")
        if hasattr(user, 'photo') and user.photo:
            res["has_avatar"] = True
            if hasattr(user.photo, 'dc_id'):
                res["dc_id"] = user.photo.dc_id
    except Exception as e:
        logger.error(f"Direct API error: {e}")
    return res

# ------------------------------------------------------------
# 6. MORAX – ОДНА ОТПРАВКА ЮЗЕРНЕЙМА, ЗАТЕМ НАЖАТИЕ КНОПОК
# ------------------------------------------------------------
async def press_morax_button_and_get(entity, button_text: str) -> Optional[str]:
    future = asyncio.get_event_loop().create_future()
    button_clicked = False

    @user_client.on(events.MessageEdited(from_users=entity.id))
    @user_client.on(events.NewMessage(from_users=entity.id))
    async def handler(event):
        nonlocal button_clicked
        msg = event.message
        if not msg.text:
            return
        if not button_clicked and msg.reply_markup:
            for row in msg.reply_markup.rows:
                for btn in row.buttons:
                    if button_text.lower() in btn.text.lower():
                        logger.info(f"Нажимаем кнопку Morax: {btn.text}")
                        try:
                            await event.click(btn)
                            button_clicked = True
                            @user_client.on(events.MessageEdited(from_users=entity.id))
                            @user_client.on(events.NewMessage(from_users=entity.id))
                            async def result_handler(res_event):
                                if not future.done() and res_event.message.text:
                                    future.set_result(res_event.message.text)
                                    user_client.remove_event_handler(result_handler)
                            return
                        except Exception as e:
                            logger.error(f"Ошибка нажатия {button_text}: {e}")
                            future.set_exception(e)
            if not button_clicked and not future.done():
                pass
        else:
            if button_clicked and not future.done() and msg.text:
                future.set_result(msg.text)

    try:
        return await asyncio.wait_for(future, timeout=TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут при получении {button_text}")
        return None
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return None
    finally:
        try:
            user_client.remove_event_handler(handler)
        except:
            pass

async def get_morax_data(username: str) -> Dict[str, Optional[str]]:
    result = {"profile": None, "gifts": None, "words": None}
    try:
        entity = await user_client.get_entity(BOT_MORAX)
    except:
        return result

    # Отправляем юзернейм, ждём меню
    future_menu = asyncio.get_event_loop().create_future()
    @user_client.on(events.NewMessage(from_users=entity.id))
    async def menu_handler(event):
        if not future_menu.done() and event.message.text:
            future_menu.set_result(True)
            user_client.remove_event_handler(menu_handler)
    await user_client.send_message(entity, username)
    try:
        await asyncio.wait_for(future_menu, timeout=TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Morax не ответил на юзернейм")
        return result
    finally:
        try:
            user_client.remove_event_handler(menu_handler)
        except:
            pass

    # Профиль
    result["profile"] = await press_morax_button_and_get(entity, "Профиль")
    await asyncio.sleep(2)
    # Подарки
    result["gifts"] = await press_morax_button_and_get(entity, "Подарки")
    await asyncio.sleep(2)
    # Частота слов
    words = await press_morax_button_and_get(entity, "Частота слов")
    if not words:
        words = await press_morax_button_and_get(entity, "Частота сл")
        if not words:
            words = await press_morax_button_and_get(entity, "Разнообразие")
    result["words"] = words

    return result

# ------------------------------------------------------------
# 7. ФОРМИРОВАНИЕ ОТЧЁТА (БЕЗ ОБЩИХ ГРУПП)
# ------------------------------------------------------------
def build_report(username: str, phone: Optional[str], vek: dict, dates: dict,
                 direct: dict, morax: dict) -> str:
    lines = []
    lines.append(f"🔍 МАКСИМАЛЬНЫЙ OSINT-ОТЧЁТ | @{username}")
    lines.append("═══════════════════════════════════════════════════════════════")
    lines.append("")

    if phone:
        lines.append(f"📞 НОМЕР ТЕЛЕФОНА\n{phone}\n")
    else:
        lines.append("📞 НОМЕР ТЕЛЕФОНА: не найден\n")

    if vek.get("operator") or vek.get("full_name"):
        lines.append("📡 ОПЕРАТОР / РЕГИОН / БАЗЫ")
        if vek.get("operator"): lines.append(vek["operator"])
        if vek.get("region"): lines.append(vek["region"])
        if vek.get("bases_count"): lines.append(f"Найдено в базах: {vek['bases_count']} записей")
        lines.append("")
        if vek.get("full_name"): lines.append(f"👤 ФИО ИЗ УТЕЧЕК\n{vek['full_name']}\n")
        if vek.get("address"): lines.append(f"🏠 АДРЕС\n{vek['address']}\n")

    lines.append("🆔 ПРЯМОЙ API TELEGRAM")
    lines.append(f"ID: {direct.get('id')}")
    if direct.get("usernames"):
        lines.append(f"Активные username: {', '.join(direct['usernames'])}")
    elif direct.get("username"):
        lines.append(f"Активные username: {direct['username']}")
    lines.append(f"Имя: {direct.get('first_name')} {direct.get('last_name') or ''}")
    lines.append(f"Био: {direct.get('bio') or '—'}")
    lines.append(f"Премиум: {'✅' if direct.get('premium') else '❌'}")
    lines.append(f"Верифицирован: {'✅' if direct.get('verified') else '❌'}")
    lines.append(f"Scam/Fake: {direct.get('scam')}/{direct.get('fake')}")
    lines.append(f"Бот: {'✅' if direct.get('bot') else '❌'}")
    lines.append(f"Аватар: {'есть' if direct.get('has_avatar') else 'нет'}")
    if direct.get("dc_id"): lines.append(f"Дата-центр: DC{direct['dc_id']}")
    lines.append("")

    lines.append("🕒 АКТИВНОСТЬ")
    lines.append(f"Статус: {direct.get('status_text')}")
    if dates.get("first_seen"): lines.append(f"Первое появление (dateregbot): {dates['first_seen']}")
    if dates.get("reg_date"): lines.append(f"Дата регистрации (dateregbot): {dates['reg_date']}")
    if direct.get("reg_date"): lines.append(f"Дата регистрации (API): {direct['reg_date']}")
    lines.append("")

    if morax.get("profile"):
        lines.append("📊 ПРОФИЛЬ (MORAX)")
        lines.append(morax["profile"].strip())
        lines.append("")
    if morax.get("gifts"):
        lines.append("🎁 ПОДАРКИ (MORAX)")
        lines.append(morax["gifts"].strip())
        lines.append("")
    if morax.get("words"):
        lines.append("📌 ЧАСТОТА СЛОВ / РАЗНООБРАЗИЕ (MORAX)")
        lines.append(morax["words"].strip())
        lines.append("")

    if not phone and not vek.get("operator") and not direct.get("id"):
        lines.append("❌ Информация не найдена.")
    return "\n".join(lines)

# ------------------------------------------------------------
# 8. ОБРАБОТЧИКИ КОМАНД
# ------------------------------------------------------------
@dp.message(Command('start'))
async def cmd_start(message: types.Message):
    await message.reply(
        "🔮 <b>Максимальный OSINT-бот (1 запрос на бота)</b>\n\n"
        "Отправь юзернейм (например, @durov).\n"
        "Собираю:\n"
        "• номер телефона (liuofxnhvm3dvqbot)\n"
        "• данные из утечек (vektok)\n"
        "• даты регистрации (dateregbot)\n"
        "• прямой API Telegram (ID, имя, статус, аватар, премиум и т.д.)\n"
        "• Morax: профиль, подарки, частота слов\n\n"
        "⏳ До 2.5 минут.\n"
        "⚠️ Каждый бот опрашивается ровно 1 раз.",
        parse_mode="HTML"
    )

@dp.message()
async def handle_username(message: types.Message):
    target = message.text.strip().lstrip('@')
    if not target:
        await message.reply("❌ Введите юзернейм.")
        return

    status = await message.reply(f"🔎 Сбор данных для @{target}... (до 2.5 минут)")

    phone = await get_phone(target)
    vek = {}
    if phone:
        vek = await get_vektok_data(phone)
    dates = await get_dates(target)
    direct = await get_direct_info(target)
    morax = await get_morax_data(target)

    report = build_report(target, phone, vek, dates, direct, morax)
    await status.edit_text(report, parse_mode="HTML", disable_web_page_preview=True)

# ------------------------------------------------------------
# 9. ЗАПУСК (с удалением вебхука)
# ------------------------------------------------------------
async def main():
    # Удаляем вебхук, если он был установлен (чтобы использовать поллинг)
    await bot.delete_webhook()
    await user_client.start()
    me = await user_client.get_me()
    logger.info(f"✅ Telethon авторизован как @{me.username}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
