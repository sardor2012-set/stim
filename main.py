import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
import time
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import threading
from urllib.parse import quote
import functools
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = "8275460864:AAF38ALOYi054ECuCJGTfGhHwUrtSBVqnSw"
WEBAPP_URL = "https://stim-p9gv.onrender.com/"
WELCOME_IMAGE_URL = FSInputFile("static/images/Banner.jpg")
REQUIRED_CHANNELS = {" Stimora Lab": "@stimora_lab", " STIM quiz": "@stim_quiz"}
ADMIN_IDS = [7592032451, 6823526508]
ADMIN_ID = 7592032451

DATABASE_URL = "postgresql://stim_user:JKHFlWhG880JMQk7rYGZA4bNCEwT9Dak@dpg-d6glon5m5p6s73b5dh40-a.oregon-postgres.render.com/stim_db_yesu"

def main_menu_keyboard(user_id=None, name=None, photo_url=None):
    domain = os.getenv('REPLIT_DEV_DOMAIN')
    base_url = f"https://{domain}" if domain else WEBAPP_URL
    keyboard = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=" Vazifalar", style="success", icon_custom_emoji_id="5282843764451195532", web_app=WebAppInfo(url=f"{base_url}/?user_id={user_id}&name={quote(str(name or ''))}&photo={quote(str(photo_url or ''))}&v={int(time.time())}"))],
        [KeyboardButton(text=" Bot haqida", style="primary", icon_custom_emoji_id="5334544901428229844"), KeyboardButton(text=" Yuqori reytinglar", style="primary", icon_custom_emoji_id="5462927083132970373")],
        [KeyboardButton(text=" Yordam", style="danger", icon_custom_emoji_id="5238025132177369293")]
    ], resize_keyboard=True, one_time_keyboard=False)
    return keyboard

def main_menu_keyboard_no_webapp():
    keyboard = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=" Menu", style="primary", icon_custom_emoji_id="5363840027245696377")]
    ], resize_keyboard=True, one_time_keyboard=False)
    return keyboard

def channels_keyboard():
    keyboard = []
    channels = list(get_all_active_channels().items())
    for i in range(0, len(channels), 2):
        row = [InlineKeyboardButton(text=channels[i][0], url=f"https://t.me/{channels[i][1][1:]}", style="primary", icon_custom_emoji_id="5224316404022415384")]
        if i + 1 < len(channels):
            row.append(InlineKeyboardButton(text=channels[i + 1][0], url=f"https://t.me/{channels[i + 1][1][1:]}", style="primary", icon_custom_emoji_id="5256235510044594825"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="Obuna bo'ldim", callback_data="check_subscription", style="success", icon_custom_emoji_id="5850654130497916523")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def check_subscription(user_id):
    try:
        for channel_name, channel_id in get_all_active_channels().items():
            try:
                member = await bot.get_chat_member(channel_id, user_id)
                if member.status not in ['member', 'administrator', 'creator']:
                    return False
            except:
                return False
        return True
    except:
        return False

async def verify_subscription(user_id, force_check=False):
    try:
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT is_subscribed, last_sub_check FROM users WHERE user_id = %s", (user_id,))
                user_data = db.fetchone()
                if not user_data:
                    return True  # Если пользователя нет, считаем подписанным

                last_check = user_data['last_sub_check']
                should_check = False

                if force_check:
                    should_check = True
                elif not last_check:
                    should_check = True
                else:
                    # Обработка разных типов для last_check
                    try:
                        if isinstance(last_check, str):
                            last_check_dt = datetime.fromisoformat(last_check.replace('Z', '+00:00'))
                        else:
                            last_check_dt = last_check

                        if (datetime.now() - last_check_dt).total_seconds() > 3600:
                            should_check = True
                    except:
                        should_check = True

                if should_check:
                    is_subscribed = await check_subscription(user_id)
                    try:
                        db.execute("UPDATE users SET is_subscribed = %s, last_sub_check = %s WHERE user_id = %s", (is_subscribed, datetime.now(), user_id))
                        conn.commit()
                    except:
                        pass
                    return is_subscribed

                return bool(user_data['is_subscribed'])
    except Exception as e:
        logger.error(f"Error in verify_subscription: {e}")
        return True  # При ошибке считаем подписанным

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

_active_channels_cache, _cache_timestamp, CACHE_DURATION = None, None, 60

def get_all_active_channels(force_refresh=False):
    global _active_channels_cache, _cache_timestamp
    current_time = datetime.now()
    if force_refresh or _active_channels_cache is None or _cache_timestamp is None or (current_time - _cache_timestamp).total_seconds() > CACHE_DURATION:
        channels = dict(REQUIRED_CHANNELS)
        try:
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'sponsors')")
                    if db.fetchone()['exists']:
                        db.execute("SELECT channel_name, channel_id FROM sponsors WHERE is_active = TRUE")
                        for sponsor in db.fetchall():
                            channels[sponsor['channel_name']] = sponsor['channel_id']
        except Exception as e:
            logger.error(f"Error fetching sponsors: {e}")
        _active_channels_cache, _cache_timestamp = channels, current_time
    return _active_channels_cache

def init_db():
    with get_db() as conn:
        with conn.cursor() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, username TEXT, is_subscribed BOOLEAN DEFAULT FALSE, last_sub_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP, first_name TEXT, last_name TEXT, class_name TEXT, is_registered BOOLEAN DEFAULT FALSE, rating INTEGER DEFAULT 0, photo_url TEXT, is_blocked BOOLEAN DEFAULT FALSE, server_nick TEXT)''')
            db.execute('''CREATE TABLE IF NOT EXISTS user_tasks (user_id BIGINT, task_id INTEGER, is_correct BOOLEAN, earned_rating INTEGER, completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, answers TEXT, correct_count INTEGER DEFAULT 0, incorrect_count INTEGER DEFAULT 0, started_at TIMESTAMP, PRIMARY KEY (user_id, task_id))''')
            db.execute('''CREATE TABLE IF NOT EXISTS sponsors (id SERIAL PRIMARY KEY, channel_name TEXT NOT NULL, channel_id TEXT NOT NULL, is_active BOOLEAN DEFAULT TRUE)''')
            db.execute('''CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, discount_percent INTEGER, category TEXT, is_one_time BOOLEAN)''')
            db.execute('''CREATE TABLE IF NOT EXISTS reviews (id SERIAL PRIMARY KEY, user_id BIGINT, username TEXT, stars INTEGER, text TEXT, review_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            db.execute('''CREATE TABLE IF NOT EXISTS purchases (purchase_id SERIAL PRIMARY KEY, user_id BIGINT, item_id INTEGER, item_name TEXT, price INTEGER, status TEXT DEFAULT 'pending', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, server_nick TEXT)''')
            db.execute('''CREATE TABLE IF NOT EXISTS task_bundles (id SERIAL PRIMARY KEY, name TEXT NOT NULL, league_id TEXT DEFAULT 'all', time_limit INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            db.execute('''CREATE TABLE IF NOT EXISTS bundle_questions (id SERIAL PRIMARY KEY, bundle_id INTEGER NOT NULL, question TEXT NOT NULL, options TEXT NOT NULL, correct_option INTEGER NOT NULL DEFAULT 0, rating INTEGER DEFAULT 5, FOREIGN KEY (bundle_id) REFERENCES task_bundles(id) ON DELETE CASCADE)''')
            db.execute('''CREATE TABLE IF NOT EXISTS system_settings (key TEXT PRIMARY KEY, value TEXT)''')
            db.execute('''CREATE TABLE IF NOT EXISTS items (item_id BIGINT PRIMARY KEY, name TEXT, price INTEGER, category TEXT, description TEXT, options TEXT, correct_option INTEGER)''')

            items_data = [
                (1, 'Что означает этот знак + %s', 10, 'all', 'Математическая задача', 'знак принадлежности|пересечение|объединение|пустое множество', 2),
                (2, 'Сколько будет 2 + 2 * 2%s', 5, 'all', 'Математика', '4|6|8|0', 1),
                (3, 'Столица Франции%s', 5, 'all', 'География', 'Берлин|Лондон|пАРИЖ|Рим', 2),
                (4, 'Самая большая планета%s', 5, 'all', 'Астрономия', 'Марс|Земля|Юпитер Сатурн', 2),
                (5, 'Химический символ золота%s', 5, 'all', 'Химия', 'Ag|Au|Fe|Cu', 1)
            ]
            for item in items_data:
                db.execute("INSERT INTO items (item_id, name, price, category, description, options, correct_option) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (item_id) DO UPDATE SET name=EXCLUDED.name, price=EXCLUDED.price, category=EXCLUDED.category, description=EXCLUDED.description, options=EXCLUDED.options, correct_option=EXCLUDED.correct_option", item)

            db.execute("SELECT value FROM system_settings WHERE key = 'season_start'")
            if not db.fetchone():
                db.execute("INSERT INTO system_settings (key, value) VALUES ('season_start', %s)", (datetime.now().isoformat(),))

            conn.commit()

SPAM_LIMIT = 5
SPAM_TIME_WINDOW = 3
SPAM_BLOCK_DURATION = 300
AUTO_BLOCK_THRESHOLD = 3
FLASK_RATE_LIMIT = 100
FLASK_RATE_WINDOW = 60
FLASK_DDOS_BLOCK_DURATION = 300

user_message_timestamps = defaultdict(list)
violation_counts = defaultdict(int)
ip_request_timestamps = defaultdict(list)
blocked_users = {}
blocked_ips = {}

class AntiSpamMiddleware(BaseMiddleware):
    def __init__(self, limit: int = SPAM_LIMIT, window: int = SPAM_TIME_WINDOW):
        self.limit = limit
        self.window = window
        super().__init__()

    async def __call__(self, handler, event, data):
        if isinstance(event, (Message, CallbackQuery)):
            user_id = event.from_user.id if event.from_user else None
            if user_id is None:
                return await handler(event, data)
            if user_id in blocked_users:
                block_time = blocked_users[user_id]
                if time.time() - block_time >= SPAM_BLOCK_DURATION:
                    del blocked_users[user_id]
                    violation_counts[user_id] = 0
                    user_message_timestamps[user_id] = []
                    logger.info(f"User {user_id} has been auto-unblocked")
                else:
                    await self._notify_blocked(event, user_id)
                    return
            current_time = time.time()
            user_message_timestamps[user_id] = [ts for ts in user_message_timestamps[user_id] if current_time - ts < self.window]
            if len(user_message_timestamps[user_id]) >= self.limit:
                violation_counts[user_id] += 1
                logger.warning(f"Spam detected from user {user_id}. Violation count: {violation_counts[user_id]}")
                if violation_counts[user_id] >= AUTO_BLOCK_THRESHOLD:
                    blocked_users[user_id] = time.time()
                    await self._block_user(user_id)
                    await self._notify_blocked(event, user_id)
                else:
                    await self._warn_user(event)
                return
            user_message_timestamps[user_id].append(current_time)
        return await handler(event, data)

    async def _warn_user(self, event):
        try:
            if isinstance(event, Message):
                await event.answer(f"<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> Juda ko‘p xabar yuboryapsiz! Iltimos, xabarlar orasida {self.window} soniya kuting.\nQoidani yana buzsangiz, bloklanasiz.", show_alert=True, parse_mode='HTML')
            elif isinstance(event, CallbackQuery):
                await event.answer(f"<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> Juda ko‘p so‘rov yuboryapsiz! {self.window} soniya kuting.", show_alert=True, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error sending warning: {e}")

    async def _notify_blocked(self, event, user_id):
        try:
            block_time = SPAM_BLOCK_DURATION // 60
            if isinstance(event, Message):
                await event.answer(f"<tg-emoji emoji-id=\"5240241223632954241\">🚫</tg-emoji> Spam uchun siz {block_time} daqiqaga bloklandingiz.\nAgar bu xatolik bo‘lsa, administratorga murojaat qiling.", show_alert=True, parse_mode='HTML')
            elif isinstance(event, CallbackQuery):
                await event.answer(f"<tg-emoji emoji-id=\"5240241223632954241\">🚫</tg-emoji> Spam uchun siz {block_time} daqiqaga bloklandingiz.", show_alert=True, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error notifying blocked user: {e}")

    async def _block_user(self, user_id):
        logger.info(f"User {user_id} has been temporarily blocked for spam (in-memory)")

async def check_and_unblock_users():
    while True:
        try:
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Error in unblock check: {e}")
            await asyncio.sleep(60)

def rate_limit_ip(limit: int = FLASK_RATE_LIMIT, window: int = FLASK_RATE_WINDOW):
    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            if client_ip in blocked_ips:
                block_time = blocked_ips[client_ip]
                if time.time() - block_time >= FLASK_DDOS_BLOCK_DURATION:
                    del blocked_ips[client_ip]
                    ip_request_timestamps[client_ip] = []
                    logger.info(f"IP {client_ip} has been auto-unblocked")
                else:
                    return jsonify({"error": "Juda ko‘p so‘rov yubordingiz. Siz 5 daqiqaga bloklandingiz.", "blocked": True}), 429
            current_time = time.time()
            ip_request_timestamps[client_ip] = [ts for ts in ip_request_timestamps[client_ip] if current_time - ts < window]
            if len(ip_request_timestamps[client_ip]) >= limit:
                blocked_ips[client_ip] = time.time()
                logger.warning(f"DDoS attempt detected from IP: {client_ip}")
                return jsonify({"error": "Juda ko‘p so‘rov yubordingiz. Iltimos, kuting.", "rate_limit_exceeded": True}), 429
            ip_request_timestamps[client_ip].append(current_time)
            return f(*args, **kwargs)
        return decorated_function
    return decorator

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return render_template('index.html', domain=os.getenv('REPLIT_DEV_DOMAIN'))

@app.route('/admin')
@app.route('/admin.html')
def admin_panel():
    return render_template('admin.html')

def get_user_row(user, key, default=None):
    return user[key] if user and key in user else default

@app.route('/api/user/<int:user_id>')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def get_user_api(user_id):
    try:
        username = request.args.get('username', f"User {user_id}")
        name = request.args.get('name', username)
        photo_param = request.args.get('photo')
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
                user = db.fetchone()
                if not user:
                    db.execute("INSERT INTO users (user_id, username, photo_url) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO NOTHING ON CONFLICT (user_id) DO NOTHING", (user_id, username, photo_param))
                    conn.commit()
                    db.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
                    user = db.fetchone()
                photo_url = get_user_row(user, 'photo_url')
                if photo_param and photo_param != photo_url:
                    db.execute("UPDATE users SET photo_url = %s WHERE user_id = %s", (photo_param, user_id))
                    conn.commit()
                    photo_url = photo_param
                rating = get_user_row(user, 'rating', 0)
                display_name = name or username
                if user:
                    f_name = get_user_row(user, 'first_name')
                    l_name = get_user_row(user, 'last_name')
                    if f_name and l_name: display_name = f"{f_name} {l_name}"
                    elif f_name: display_name = f_name
                class_name = get_user_row(user, 'class_name')
                league, league_place, league_rank, top_players = "Boshlang'ich liga", "Hammasi", None, []
                league_classes = []
                if class_name:
                    clean_class = ''.join(filter(str.isdigit, class_name))
                    if clean_class:
                        class_num = int(clean_class)
                        if 1 <= class_num <= 4:
                            league, league_place, league_classes = "Boshlang'ich liga", "1-4 sinf", ['1', '2', '3', '4', '1 класс', '2 класс', '3 класс', '4 класс']
                        elif 5 <= class_num <= 6:
                            league, league_place, league_classes = "Bronza liga", "5-6 sinf", ['5', '6', '5 класс', '6 класс']
                        elif 7 <= class_num <= 8:
                            league, league_place, league_classes = "Kumush liga", "7-8 sinf", ['7', '8', '7 класс', '8 класс']
                        elif 9 <= class_num <= 11:
                            league, league_place, league_classes = "Oltin liga", "9-11 sinf", ['9', '10', '11', '9 класс', '10 класс', '11 класс']
                if not league_classes: league, league_place = "Umumiy liga", "Barcha sinflar"
                user_rating_value = rating
                if league_classes:
                    placeholders = ','.join(['%s' for _ in league_classes])
                    clean_league_nums = [''.join(filter(str.isdigit, c)) for c in league_classes if ''.join(filter(str.isdigit, c))]
                    count_query = f"SELECT COUNT(*) as cnt FROM users WHERE (class_name IN ({placeholders})"
                    if clean_league_nums: count_query += f" OR {' OR '.join(['class_name LIKE %s' for _ in range(len(clean_league_nums)*2)])}"
                    count_query += ") AND (rating > %s OR (rating = %s AND user_id < %s))"
                    count_params = list(league_classes) + [f"{n}%" for n in clean_league_nums] + [f"%{n}" for n in clean_league_nums] + [user_rating_value, user_rating_value, user_id]
                    db.execute(count_query, count_params)
                    higher_count = db.fetchone()
                    league_rank = (higher_count['cnt'] or 0) + 1
                    query = f"SELECT user_id, rating, first_name, last_name, username, photo_url FROM users WHERE (class_name IN ({placeholders})"
                    if clean_league_nums: query += f" OR {' OR '.join(['class_name LIKE %s' for _ in range(len(clean_league_nums)*2)])}"
                    query += ") ORDER BY rating DESC, user_id ASC LIMIT 3"
                    db.execute(query, count_params[:-3])
                    league_top = db.fetchall()
                    for idx, lu in enumerate(league_top, 1):
                        p_name = f"{lu['first_name']} {lu['last_name']}" if lu['first_name'] else (lu['username'] or f"User {lu['user_id']}")
                        top_players.append({"user_id": lu['user_id'], "name": p_name, "rating": lu['rating'], "rank": idx, "photo": get_user_row(lu, 'photo_url')})
                else:
                    db.execute("SELECT COUNT(*) as cnt FROM users WHERE (rating > %s OR (rating = %s AND user_id < %s))", (user_rating_value, user_rating_value, user_id))
                    higher_count = db.fetchone()
                    league_rank = (higher_count['cnt'] or 0) + 1
                    db.execute("SELECT user_id, rating, first_name, last_name, username, photo_url FROM users ORDER BY rating DESC, user_id ASC LIMIT 3")
                    all_users = db.fetchall()
                    for idx, lu in enumerate(all_users, 1):
                        p_name = f"{lu['first_name']} {lu['last_name']}" if lu['first_name'] else (lu['username'] or f"User {lu['user_id']}")
                        top_players.append({"user_id": lu['user_id'], "name": p_name, "rating": lu['rating'], "rank": idx, "photo": get_user_row(lu, 'photo_url')})
                all_leagues_tops = {}
                leagues_config = {"Бронзовая лига": ['5', '6', '5 класс', '6 класс'], "Серебряная лига": ['7', '8', '7 класс', '8 класс'], "Золотая лига": ['9', '10', '11', '9 класс', '10 класс', '11 класс']}
                for l_name, l_classes in leagues_config.items():
                    placeholders = ','.join(['%s' for _ in l_classes])
                    clean_league_nums = [''.join(filter(str.isdigit, c)) for c in l_classes if ''.join(filter(str.isdigit, c))]
                    query = f"SELECT user_id, rating, first_name, last_name, username, photo_url FROM users WHERE (class_name IN ({placeholders})"
                    if clean_league_nums: query += f" OR {' OR '.join(['class_name LIKE %s' for _ in range(len(clean_league_nums)*2)])}"
                    query += ") ORDER BY rating DESC LIMIT 3"
                    params = list(l_classes) + [f"{n}%" for n in clean_league_nums] + [f"%{n}" for n in clean_league_nums]
                    db.execute(query, params)
                    tops = db.fetchall()
                    all_leagues_tops[l_name] = [{"user_id": t['user_id'], "name": f"{t['first_name']} {t['last_name']}" if t['first_name'] else (t['username'] or f"User {t['user_id']}"), "rating": t['rating'], "photo": get_user_row(t, 'photo_url')} for t in tops]
                season_days = 30
                db.execute("SELECT value FROM system_settings WHERE key = 'season_start'")
                season_start_val = db.fetchone()
                if season_start_val:
                    try:
                        start_dt = datetime.fromisoformat(season_start_val['value'])
                        season_days = max(0, 30 - (datetime.now() - start_dt).days)
                    except: pass
                return jsonify({"user_id": user_id, "username": get_user_row(user, 'username') or username, "first_name": get_user_row(user, 'first_name'), "last_name": get_user_row(user, 'last_name'), "class_name": class_name, "is_registered": bool(get_user_row(user, 'is_registered', False)), "rating": rating, "referrals": rating, "status": "Boshlang'ich" if rating < 50 else ("O'rganuvchi" if rating < 100 else "Master"), "league": league, "league_place": league_place, "league_rank": league_rank, "display_name": display_name, "photo": photo_url, "is_admin": str(user_id) in [str(id) for id in ADMIN_IDS], "server_nick": get_user_row(user, 'server_nick'), "top_players": top_players, "all_leagues_tops": all_leagues_tops, "days_left": season_days, "is_blocked": bool(get_user_row(user, 'is_blocked', False))})
    except Exception as e:
        logger.error(f"Error in get_user_api: {e}")
        return jsonify({"message": str(e)}), 500

@app.route('/api/register', methods=['POST'])
@rate_limit_ip(limit=20, window=60)
def register_user():
    try:
        data = request.json
        user_id, first_name, last_name, class_name = data.get('user_id'), data.get('first_name'), data.get('last_name'), data.get('class_name')
        if not all([user_id, first_name, last_name, class_name]): return jsonify({"success": False, "message": "Missing data"}), 400
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("UPDATE users SET first_name = %s, last_name = %s, class_name = %s, is_registered = TRUE WHERE user_id = %s", (first_name, last_name, class_name, user_id))
                conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/tasks')
@rate_limit_ip(limit=50, window=60)
def get_tasks_api():
    try:
        user_id = request.args.get('user_id', type=int)
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT * FROM items")
                items = db.fetchall()
                completed_tasks = []
                if user_id:
                    db.execute("SELECT task_id FROM user_tasks WHERE user_id = %s", (user_id,))
                    completed_tasks = [r['task_id'] for r in db.fetchall()]
                tasks_list = []
                for it in items:
                    tasks_list.append({"id": it['item_id'], "title": it['name'], "rating": it['price'], "category": it['category'], "description": it['description'], "options": it['options'].split('|'), "completed": it['item_id'] in completed_tasks})
                db.execute("SELECT * FROM task_bundles")
                bundles = db.fetchall()
                bundles_list = []
                for b in bundles:
                    db.execute("SELECT COUNT(*) as cnt FROM bundle_questions WHERE bundle_id = %s", (b['id'],))
                    q_count = db.fetchone()['cnt']
                    is_completed = False
                    if user_id:
                        db.execute("SELECT 1 FROM user_tasks WHERE user_id = %s AND task_id = %s", (user_id, b['id'] + 1000))
                        is_completed = bool(db.fetchone())
                    bundles_list.append({"id": b['id'], "name": b['name'], "league": b['league_id'], "time_limit": b.get('time_limit', 0), "questions_count": q_count, "completed": is_completed})
                return jsonify({"tasks": tasks_list, "bundles": bundles_list})
    except Exception as e:
        logger.error(f"Error in get_tasks_api: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/user/<int:user_id>/tasks')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def get_user_tasks(user_id):
    try:
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT class_name FROM users WHERE user_id = %s", (user_id,))
                user = db.fetchone()
                user_class = user['class_name'] if user else None
                user_league = None
                if user_class:
                    match = re.search(r'(\d+)', user_class)
                    if match:
                        class_num = int(match.group(1))
                        if 5 <= class_num <= 6: user_league = 'bronza'
                        elif 7 <= class_num <= 8: user_league = 'kumush'
                        elif 9 <= class_num <= 11: user_league = 'oltin'

                db.execute("SELECT * FROM task_bundles ORDER BY id DESC")
                all_bundles = db.fetchall()
                db.execute("SELECT task_id FROM user_tasks WHERE user_id = %s", (user_id,))
                completed_rows = db.fetchall()
                completed_ids = [row['task_id'] for row in completed_rows]

                active_list = []
                for b in all_bundles:
                    bundle_id = b['id'] + 1000
                    bundle_league = b['league_id'] if 'league_id' in b else None
                    if bundle_league and user_league and bundle_league != user_league:
                        continue
                    if bundle_id not in completed_ids:
                        db.execute("SELECT * FROM bundle_questions WHERE bundle_id = %s", (b['id'],))
                        questions = db.fetchall()
                        if questions:
                            questions_list = [{"id": q['id'], "question": q['question'], "options": q['options'], "correct_option": q['correct_option'], "rating": q['rating'] or 5} for q in questions]
                            total_rating = sum(q['rating'] or 5 for q in questions)
                            time_limit = b.get('time_limit', 0)
                            active_list.append({"item_id": bundle_id, "name": b['name'], "category": "bundle", "questions": questions_list, "price": total_rating, "total_questions": len(questions_list), "time_limit": time_limit})

                completed_list = []
                db.execute("SELECT * FROM user_tasks WHERE user_id = %s ORDER BY completed_at DESC", (user_id,))
                completed_details = db.fetchall()
                for row in completed_details:
                    task_id = row['task_id']
                    if task_id >= 1000:
                        bundle_db_id = task_id - 1000
                        db.execute("SELECT name FROM task_bundles WHERE id = %s", (bundle_db_id,))
                        bundle = db.fetchone()
                        if bundle:
                            completed_list.append({"task_id": task_id, "name": bundle['name'], "earned_rating": row['earned_rating'] or 0, "correct_count": row['correct_count'] or 0, "incorrect_count": row['incorrect_count'] or 0, "answers": row['answers'] or '{}', "is_correct": bool(row['is_correct']) if row['is_correct'] is not None else False})

                return jsonify({"active": active_list, "completed": completed_list})
    except Exception as e:
        logger.error(f"Error getting user tasks: {e}")
        return jsonify({"active": [], "completed": []}), 500

@app.route('/api/tasks/complete', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def complete_tasks():
    try:
        data = request.json
        raw_user_id = data.get('user_id')
        try:
            user_id = int(raw_user_id) if raw_user_id is not None else None
        except (ValueError, TypeError):
            user_id = None
        score, task_id = data.get('score', 0), data.get('task_id')
        is_correct, answers = data.get('is_correct', True), data.get('answers', {})
        correct_count, incorrect_count = data.get('correct_count', 0), data.get('incorrect_count', 0)

        if user_id is None:
            return jsonify({"success": False, "message": "Missing or invalid user_id"}), 400

        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
                if not db.fetchone():
                    return jsonify({"success": False, "message": "User not found"}), 404

                # Validate score - get max possible rating for this bundle
                bundle_id = None
                if task_id and task_id >= 1000:
                    bundle_id = task_id - 1000
                elif task_id:
                    bundle_id = task_id

                max_rating = 0
                if bundle_id:
                    db.execute("SELECT COALESCE(SUM(rating), 0) as total FROM bundle_questions WHERE bundle_id = %s", (bundle_id,))
                    result = db.fetchone()
                    max_rating = result['total'] if result else 0

                # Also check items table for single questions
                if not max_rating and task_id and task_id < 1000:
                    db.execute("SELECT price FROM items WHERE item_id = %s", (task_id,))
                    item = db.fetchone()
                    if item:
                        max_rating = item['price']

                # Cap the score to maximum possible rating
                if score > max_rating:
                    logger.warning(f"User {user_id} tried to submit score {score} but max is {max_rating}. Capping.")
                    score = max_rating if max_rating > 0 else 0

                # Ensure score is not negative
                if score < 0:
                    score = 0

                db.execute("UPDATE users SET rating = rating + %s WHERE user_id = %s", (score, user_id))
                if task_id:
                    # Check if task is already completed
                    db.execute("SELECT earned_rating FROM user_tasks WHERE user_id = %s AND task_id = %s AND completed_at IS NOT NULL", (user_id, task_id))
                    existing_completed = db.fetchone()

                    if existing_completed:
                        # Task already completed - return error to prevent duplicate rating
                        conn.rollback()
                        return jsonify({"success": False, "message": "Bu vazifa allaqachon bajarilgan"}), 400

                    db.execute("""
                        INSERT INTO user_tasks (user_id, task_id, is_correct, earned_rating, answers, correct_count, incorrect_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, task_id) DO UPDATE SET
                            is_correct = EXCLUDED.is_correct,
                            earned_rating = EXCLUDED.earned_rating,
                            answers = EXCLUDED.answers,
                            correct_count = EXCLUDED.correct_count,
                            incorrect_count = EXCLUDED.incorrect_count,
                            completed_at = CURRENT_TIMESTAMP
                    """, (user_id, task_id, is_correct, score, json.dumps(answers), correct_count, incorrect_count))
                conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error completing tasks: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/bundle/start', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def start_bundle():
    try:
        data = request.json
        user_id, task_id = data.get('user_id'), data.get('task_id')
        if not user_id or not task_id:
            return jsonify({"success": False, "message": "Missing user_id or task_id"}), 400

        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT started_at FROM user_tasks WHERE user_id = %s AND task_id = %s", (user_id, task_id))
                existing = db.fetchone()
                if existing and existing['started_at']:
                    return jsonify({"success": True, "started_at": str(existing['started_at']), "message": "Already started"})
                now = datetime.now()
                db.execute("""
                    INSERT INTO user_tasks (user_id, task_id, started_at, completed_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, task_id) DO UPDATE SET
                        started_at = EXCLUDED.started_at
                """, (user_id, task_id, now, now))
                conn.commit()
                return jsonify({"success": True, "started_at": now.isoformat()})
    except Exception as e:
        logger.error(f"Error starting bundle: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/items')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def get_items():
    try:
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT * FROM items")
                items = db.fetchall()
                return jsonify([{"item_id": item['item_id'], "name": item['name'], "price": item['price'], "category": item['category'], "description": item['description'], "options": item['options'], "correct_option": item['correct_option']} for item in items])
    except Exception as e:
        logger.error(f"Error getting items: {e}")
        return jsonify([]), 500

@app.route('/api/user/nickname', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def set_nickname():
    try:
        data = request.json
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("UPDATE users SET server_nick = %s WHERE user_id = %s", (data.get('nickname'), data.get('user_id')))
                conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/buy', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def buy_item():
    try:
        data = request.json
        user_id = data.get('user_id')
        item_id = data.get('item_id')
        if not user_id or not item_id:
            return jsonify({"success": False, "message": "Missing parameters"}), 400
        return jsonify({"success": True, "message": "Purchase successful"})
    except Exception as e:
        logger.error(f"Error in buy_item: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/review', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def submit_review():
    try:
        data = request.json
        user_id = data.get('user_id')
        stars = data.get('stars')
        text = data.get('text')
        if not user_id or stars is None:
            return jsonify({"success": False, "message": "Missing parameters"}), 400
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("INSERT INTO reviews (user_id, stars, text) VALUES (%s, %s, %s)", (user_id, stars, text or ''))
                conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error in submit_review: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/user/<int:user_id>/titles')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def get_user_titles(user_id):
    return jsonify({"titles": []})

@app.route('/api/titles/buy', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def buy_title():
    try:
        data = request.json
        return jsonify({"success": True, "message": "Title purchased"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/titles/activate', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def activate_title():
    try:
        data = request.json
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/promo/check')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def check_promo():
    try:
        code = request.args.get('code')
        item_id = request.args.get('item_id')
        user_id = request.args.get('user_id')
        return jsonify({"valid": False, "discount": 0})
    except Exception as e:
        return jsonify({"valid": False, "discount": 0}), 500

bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)
spam_middleware = AntiSpamMiddleware(limit=SPAM_LIMIT, window=SPAM_TIME_WINDOW)
dp.message.middleware.register(spam_middleware)
dp.callback_query.middleware.register(spam_middleware)

@router.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    if not user:
        return
    logger.info(f"Command /start from user {user.id}")

    try:
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("INSERT INTO users (user_id, username, first_name, last_name) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING", (user.id, user.username or "", user.first_name or "", user.last_name or ""))
                conn.commit()

        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT is_blocked FROM users WHERE user_id = %s", (user.id,))
                db_user = db.fetchone()
                if db_user and db_user['is_blocked']:
                    await message.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji> <b>Kirish taqiqlangan</b>\n\nSizning akkauntingiz bu botda bloklangan.\nBlokdan chiqarish uchun administratorga murojaat qiling:\n\n<i>Murojaat uchun ID: {user.id}</i>", parse_mode='HTML')
                    return

        # Сначала отправляем сообщение о проверке подписки
        checking_msg = await message.answer("<tg-emoji emoji-id=\"5370935802844946281\">🔄</tg-emoji> Obunalar tekshirilmoqda...", parse_mode='HTML')

        # Запускаем асинхронную проверку подписки без блокировки
        asyncio.create_task(check_and_notify_subscription(user.id, message, checking_msg))

    except Exception as e:
        logger.error(f"Critical error in start command: {e}")
        try:
            await message.answer(f"Salom, {user.first_name}! Yuklashda xatolik yuz berdi, lekin menuni ochishingiz mumkin:", reply_markup=main_menu_keyboard(user.id, user.first_name, None))
        except:
            pass

async def check_and_notify_subscription(user_id, message: Message, checking_msg: Message):
    """Асинхронная проверка подписки и отправка соответствующего ответа"""
    try:
        # Небольшая задержка перед проверкой
        await asyncio.sleep(1)
        is_subscribed = await verify_subscription(user_id, force_check=True)

        # Удаляем сообщение о проверке
        try:
            await checking_msg.delete()
        except:
            pass

        if not is_subscribed:
            # Если не подписан - отправляем сообщение с требованием подписки
            channels = get_all_active_channels()
            channels_text = "<tg-emoji emoji-id=\"5424818078833715060\">📢</tg-emoji> <b>Kanalga obuna bo'lishingiz kerak!</b>\n\nBotdan foydalanish uchun quyidagi kanallarga obuna bo'ling:\n\n"
            for name, channel_id in channels.items():
                channels_text += f"• {name}: https://t.me/{channel_id[1:]}\n"
            channels_text += "\n<tg-emoji emoji-id=\"5850654130497916523\">✅</tg-emoji> Obuna bo'ldim"
            await message.answer(channels_text, parse_mode='HTML', reply_markup=channels_keyboard())
        else:
            # Если подписан - отправляем приветственное сообщение
            user = message.from_user
            reply_markup = main_menu_keyboard(user_id, user.first_name, None)
            text = f"<tg-emoji emoji-id=\"5413694143601842851\">👋</tg-emoji> Salom, <b>{user.first_name}</b>! STIM quiz botiga xush kelibsiz! <tg-emoji emoji-id=\"5992459729975122233\">📱</tg-emoji>\n\n<tg-emoji emoji-id=\"5406745015365943482\">👇</tg-emoji> <b>Quyidagi tugmalardan foydalaning:</b>"

            try:
                if WELCOME_IMAGE_URL:
                    await message.answer_photo(photo=WELCOME_IMAGE_URL, caption=text, reply_markup=reply_markup, parse_mode='HTML')
                else:
                    await message.answer(text=text, reply_markup=reply_markup, parse_mode='HTML')
            except Exception as e:
                logger.error(f"Error sending welcome photo: {e}")
                await message.answer(text=text, reply_markup=reply_markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error in check_and_notify_subscription: {e}")
        try:
            await checking_msg.delete()
        except:
            pass
        # Отправляем сообщение об ошибке с меню
        try:
            user = message.from_user
            await message.answer(f"Salom, {user.first_name}! Yuklashda xatolik yuz berdi, lekin menuni ochishingiz mumkin:", reply_markup=main_menu_keyboard(user.id, user.first_name, None))
        except:
            pass


@router.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id

    is_subscribed = await verify_subscription(user_id, force_check=True)

    if is_subscribed:
        # Удаляем сообщение с запросом подписки
        try:
            await callback.message.delete()
        except:
            pass

        reply_markup = main_menu_keyboard(user_id, callback.from_user.first_name, None)
        text = f"<tg-emoji emoji-id=\"5413694143601842851\">👋</tg-emoji> Salom, <b>{callback.from_user.first_name}</b>! STIM quiz botiga xush kelibsiz! <tg-emoji emoji-id=\"5992459729975122233\">📱</tg-emoji>\n\n<tg-emoji emoji-id=\"5406745015365943482\">👇</tg-emoji> <b>Quyidagi tugmalardan foydalaning:</b>"
        try:
            if WELCOME_IMAGE_URL:
                await callback.message.answer_photo(photo=WELCOME_IMAGE_URL, caption=text, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await callback.message.answer(text=text, reply_markup=reply_markup, parse_mode='HTML')
        except:
            await callback.message.answer(text=text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        await callback.answer("Siz hali kanallarga obuna bo'lmadingiz! Iltimos, obuna bo'ling va qayta urining.", show_alert=True)

@router.message(F.text == "Bot haqida")
async def menu_about(message: Message):
    about_text = "<tg-emoji emoji-id=\"5334544901428229844\">ℹ️</tg-emoji> <b>Bot haqida</b>\n\nBu reyting tizimiga ega bo'lgan ta'limiy bot.\nVazifalarni bajaring, reytingni oshiring va boshqa ishtirokchilar bilan bellashing!\n\n<tg-emoji emoji-id=\"5231200819986047254\">📊</tg-emoji> <b>Funksiyalar:</b>\n• Turli toifadagi vazifalarni yechish\n• Reyting tizimi\n• Sinflar / ligalar bo'yicha bo'linish\n• Yutuqlar uchun mukofotlar\n\n<tg-emoji emoji-id=\"5397782960512444700\">🧐</tg-emoji> <b>Samarqand tuman ixtisoslashtirilgan maktab</b> tomonidan ta'limiy maqsadlarda ishlab chiqilgan."
    await message.answer(about_text, parse_mode='HTML', reply_markup=main_menu_keyboard_no_webapp())

@router.message(F.text == "Yuqori reytinglar")
async def menu_top_ratings(message: Message):
    try:
        with get_db() as conn:
            with conn.cursor() as db:
                db.execute("SELECT user_id, username, first_name, last_name, rating, class_name FROM users WHERE rating > 0 ORDER BY rating DESC LIMIT 50")
                top_users = db.fetchall()

        if not top_users:
            await message.answer("<tg-emoji emoji-id=\"5462927083132970373\">🏆</tg-emoji> Reyting hali bo'sh. Birinchi bo'ling!", parse_mode='HTML', reply_markup=main_menu_keyboard_no_webapp())
            return
        rating_text = "<tg-emoji emoji-id=\"5462927083132970373\">🏆</tg-emoji>  <b>TOP 50 REYTINGLAR</b>\n\n"
        medals = ["🥇 ", "🥈 ", "🥉 "]
        for idx, user in enumerate(top_users, 1):
            name = f"{user['first_name']} {user['last_name']}" if user['first_name'] else (user['username'] or f"User {user['user_id']}")
            medal = medals[idx - 1] if idx <= 3 else ""
            rating_text += f"{idx}. {medal}<b>{name}</b> - {user['rating']} reyting"
            if user['class_name']:
                rating_text += f" ({user['class_name']})"
            rating_text += "\n"
        if len(rating_text) > 4000:
            for part in [rating_text[i:i+4000] for i in range(0, len(rating_text), 4000)]:
                await message.answer(part, parse_mode='HTML', reply_markup=main_menu_keyboard_no_webapp())
        else:
            await message.answer(rating_text, parse_mode='HTML', reply_markup=main_menu_keyboard_no_webapp())
    except Exception as e:
        logger.error(f"Error in top_ratings: {e}")
        await message.answer("❌ Reytingni olishda xatolik yuz berdi.", reply_markup=main_menu_keyboard_no_webapp())

@router.message(F.text == "Yordam")
async def menu_help(message: Message):
    support_text = "<tg-emoji emoji-id=\"5334544901428229844\">🆘</tg-emoji> <b>Yordam (Qo'llab-quvvatlash)</b>\n\nAgar sizda muammo yoki savollar bo'lsa:\n\n<tg-emoji emoji-id=\"5253742260054409879\">📧</tg-emoji> <b>Administrator bilan bog'lanish:</b>\n• @s_narzimurodov ga yozing\n\n<tg-emoji emoji-id=\"5823268688874179761\">🔧</tg-emoji> <b>Ko'p beriladigan savollar:</b>\n• <i>Vazifalarni qanday boshlash mumkin?</i> - 'Vazifalar' tugmasini bosing\n• <i>Reytingni qanday oshirish mumkin?</i> - Vazifalarni to'g'ri bajaring\n• <i>Ro'yxatdan o'tishda muammo bormi?</i> - Administratorga murojaat qiling\n\n<tg-emoji emoji-id=\"5224607267797606837\">⚡</tg-emoji> <b>Biz har doim yordam berishga tayyormiz!</b>"
    await message.answer(support_text, parse_mode='HTML', reply_markup=main_menu_keyboard_no_webapp())

@router.message(F.text == "Menu")
async def menu_tasks(message: Message):
    user = message.from_user
    reply_markup = main_menu_keyboard(user.id, user.first_name, None)
    text = f"<tg-emoji emoji-id=\"5406745015365943482\">👇</tg-emoji> <b>Menuga xush kelibsiz, {user.first_name}!</b>"
    await message.answer(text, parse_mode='HTML', reply_markup=reply_markup)

async def main():
    init_db()

    # ==================== ADMIN API ROUTES ====================

    @app.route('/api/admin/stats')
    @rate_limit_ip(limit=50, window=60)
    def get_admin_stats():
        try:
            with get_db() as conn:
                with conn.cursor() as db:
                    # Total users
                    db.execute("SELECT COUNT(*) as cnt FROM users")
                    total_users = db.fetchone()['cnt'] or 0

                    # Registered users
                    db.execute("SELECT COUNT(*) as cnt FROM users WHERE is_registered = TRUE")
                    registered_users = db.fetchone()['cnt'] or 0

                    # Total tasks completed
                    db.execute("SELECT COUNT(*) as cnt FROM user_tasks")
                    total_tasks_completed = db.fetchone()['cnt'] or 0

                    # Total rating
                    db.execute("SELECT COALESCE(SUM(rating), 0) as total FROM users")
                    total_rating = db.fetchone()['total'] or 0

                    # New users today
                    db.execute("SELECT COUNT(*) as cnt FROM users WHERE DATE(last_sub_check) = CURRENT_DATE OR DATE(last_sub_check) = CURRENT_DATE")
                    new_today = db.fetchone()['cnt'] or 0

                    # Average rating
                    db.execute("SELECT COALESCE(AVG(rating), 0) as avg FROM users WHERE rating > 0")
                    avg_rating = round(db.fetchone()['avg'] or 0, 1)

                    # League counts
                    db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('5', '6', '5 класс', '6 класс')")
                    bronze_league = db.fetchone()['cnt'] or 0

                    db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('7', '8', '7 класс', '8 класс')")
                    silver_league = db.fetchone()['cnt'] or 0

                    db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('9', '10', '11', '9 класс', '10 класс', '11 класс')")
                    gold_league = db.fetchone()['cnt'] or 0

                    # Top users
                    db.execute("SELECT user_id, username, first_name, last_name, rating, class_name FROM users ORDER BY rating DESC LIMIT 10")
                    top_users = db.fetchall()

                    # Days left in season
                    db.execute("SELECT value FROM system_settings WHERE key = 'season_start'")
                    season_start_val = db.fetchone()
                    days_left = 30
                    if season_start_val:
                        try:
                            start_dt = datetime.fromisoformat(season_start_val['value'])
                            days_left = max(0, 30 - (datetime.now() - start_dt).days)
                        except:
                            pass

                    return jsonify({
                        "total_users": total_users,
                        "registered_users": registered_users,
                        "total_tasks_completed": total_tasks_completed,
                        "total_rating": total_rating,
                        "new_today": new_today,
                        "avg_rating": avg_rating,
                        "bronze_league": bronze_league,
                        "silver_league": silver_league,
                        "gold_league": gold_league,
                        "top_users": [{"user_id": u['user_id'], "username": u['username'], "first_name": u['first_name'], "last_name": u['last_name'], "rating": u['rating'], "class_name": u['class_name']} for u in top_users],
                        "days_left": days_left
                    })
        except Exception as e:
            logger.error(f"Error in get_admin_stats: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route('/api/admin/users')
    @rate_limit_ip(limit=50, window=60)
    def get_admin_users():
        try:
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("SELECT user_id, username, first_name, last_name, class_name, rating, is_registered, is_blocked FROM users ORDER BY user_id DESC")
                    users = db.fetchall()
                    return jsonify([{"user_id": u['user_id'], "username": u['username'], "first_name": u['first_name'], "last_name": u['last_name'], "class_name": u['class_name'], "rating": u['rating'], "is_registered": u['is_registered'], "is_blocked": u['is_blocked']} for u in users])
        except Exception as e:
            logger.error(f"Error in get_admin_users: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route('/api/admin/user/<int:user_id>/block', methods=['POST'])
    @rate_limit_ip(limit=20, window=60)
    def block_admin_user(user_id):
        try:
            action = request.args.get('action', 'block')
            is_blocked = action == 'block'
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("UPDATE users SET is_blocked = %s WHERE user_id = %s", (is_blocked, user_id))
                    conn.commit()
            return jsonify({"success": True, "is_blocked": is_blocked})
        except Exception as e:
            logger.error(f"Error in block_admin_user: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    @app.route('/api/admin/user/<int:user_id>/update', methods=['POST'])
    @rate_limit_ip(limit=20, window=60)
    def update_admin_user(user_id):
        try:
            data = request.json
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("""
                        UPDATE users SET 
                            username = COALESCE(%s, username),
                            first_name = COALESCE(%s, first_name),
                            last_name = COALESCE(%s, last_name),
                            class_name = COALESCE(%s, class_name),
                            rating = COALESCE(%s, rating)
                        WHERE user_id = %s
                    """, (data.get('username'), data.get('first_name'), data.get('last_name'), data.get('class_name'), data.get('rating'), user_id))
                    conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"Error in update_admin_user: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    @app.route('/api/admin/user/<int:user_id>/delete', methods=['POST'])
    @rate_limit_ip(limit=20, window=60)
    def delete_admin_user(user_id):
        try:
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("DELETE FROM user_tasks WHERE user_id = %s", (user_id,))
                    db.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
                    conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"Error in delete_admin_user: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    @app.route('/api/admin/bundles')
    @rate_limit_ip(limit=50, window=60)
    def get_admin_bundles():
        try:
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("SELECT * FROM task_bundles ORDER BY id DESC")
                    bundles = db.fetchall()
                    result = []
                    for b in bundles:
                        db.execute("SELECT * FROM bundle_questions WHERE bundle_id = %s", (b['id'],))
                        questions = db.fetchall()
                        result.append({
                            "id": b['id'],
                            "name": b['name'],
                            "league_id": b['league_id'],
                            "time_limit": b.get('time_limit', 0),
                            "questions": [{"question": q['question'], "options": q['options'], "correct_option": q['correct_option'], "rating": q['rating']} for q in questions]
                        })
                    return jsonify(result)
        except Exception as e:
            logger.error(f"Error in get_admin_bundles: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route('/api/admin/bundle', methods=['POST'])
    @rate_limit_ip(limit=20, window=60)
    def create_admin_bundle():
        try:
            data = request.json
            name = data.get('name')
            league_id = data.get('league_id', 'all')
            time_limit = data.get('time_limit', 0)
            questions = data.get('questions', [])

            if not name:
                return jsonify({"success": False, "message": "Name is required"}), 400

            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("INSERT INTO task_bundles (name, league_id, time_limit) VALUES (%s, %s, %s) RETURNING id", (name, league_id, time_limit))
                    bundle_id = db.fetchone()['id']

                    for q in questions:
                        db.execute("""
                            INSERT INTO bundle_questions (bundle_id, question, options, correct_option, rating)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (bundle_id, q.get('question'), q.get('options'), q.get('correct_option', 0), q.get('rating', 5)))

                    conn.commit()

            return jsonify({"success": True, "id": bundle_id})
        except Exception as e:
            logger.error(f"Error in create_admin_bundle: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    @app.route('/api/admin/bundle/<int:bundle_id>', methods=['PUT'])
    @rate_limit_ip(limit=20, window=60)
    def update_admin_bundle(bundle_id):
        try:
            data = request.json
            name = data.get('name')
            league_id = data.get('league_id', 'all')
            time_limit = data.get('time_limit', 0)
            questions = data.get('questions', [])

            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("UPDATE task_bundles SET name = %s, league_id = %s, time_limit = %s WHERE id = %s", (name, league_id, time_limit, bundle_id))

                    # Delete old questions and add new ones
                    db.execute("DELETE FROM bundle_questions WHERE bundle_id = %s", (bundle_id,))

                    for q in questions:
                        db.execute("""
                            INSERT INTO bundle_questions (bundle_id, question, options, correct_option, rating)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (bundle_id, q.get('question'), q.get('options'), q.get('correct_option', 0), q.get('rating', 5)))

                    conn.commit()

            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"Error in update_admin_bundle: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    @app.route('/api/admin/bundle/<int:bundle_id>', methods=['DELETE'])
    @rate_limit_ip(limit=20, window=60)
    def delete_admin_bundle(bundle_id):
        try:
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("DELETE FROM bundle_questions WHERE bundle_id = %s", (bundle_id,))
                    db.execute("DELETE FROM task_bundles WHERE id = %s", (bundle_id,))
                    conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"Error in delete_admin_bundle: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    @app.route('/api/admin/reset-season', methods=['POST'])
    @rate_limit_ip(limit=5, window=60)
    def reset_season():
        try:
            with get_db() as conn:
                with conn.cursor() as db:
                    db.execute("UPDATE users SET rating = 0")
                    db.execute("DELETE FROM user_tasks")
                    db.execute("UPDATE system_settings SET value = %s WHERE key = 'season_start'", (datetime.now().isoformat(),))
                    conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"Error in reset_season: {e}")
            return jsonify({"success": False, "message": str(e)}), 500

    # ==================== END ADMIN API ROUTES ====================

    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000))
    flask_thread.daemon = True
    flask_thread.start()
    asyncio.create_task(check_and_unblock_users())
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
