import asyncio
import logging
import sqlite3
import os
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import threading
from urllib.parse import quote
import functools

import github_backup

TOKEN = "8580149302:AAGd1_sL75AA4HjCnvGtG3-vRDd9Nt42L0M"
WEBAPP_URL = "https://stim-y19b.onrender.com/"
WELCOME_IMAGE_URL = "https://ibb.co/CsVxsv24"
REQUIRED_CHANNELS = {" Stimora Lab": "@stimora_lab", " STIM quiz": "@stim_quiz"}
ADMIN_ID = 7592032451

# ==================== ANTI-SPAM & ANTI-DDOS CONFIG ====================
# Anti-spam settings for Telegram bot
SPAM_LIMIT = 5  # Max messages per time window
SPAM_TIME_WINDOW = 3  # Time window in seconds
SPAM_BLOCK_DURATION = 300  # Block duration in seconds (5 minutes)
AUTO_BLOCK_THRESHOLD = 3  # Number of violations before auto-block

# Anti-DDoS settings for Flask
FLASK_RATE_LIMIT = 100  # Max requests per time window
FLASK_RATE_WINDOW = 60  # Time window in seconds
FLASK_DDOS_BLOCK_DURATION = 300  # Block duration in seconds

# In-memory storage for rate limiting
user_message_timestamps = defaultdict(list)
violation_counts = defaultdict(int)
ip_request_timestamps = defaultdict(list)
blocked_users = {}  # Dict of blocked user IDs with block timestamp
blocked_ips = {}  # Dict of blocked IPs with block timestamp


# ==================== ANTI-SPAM MIDDLEWARE ====================
class AntiSpamMiddleware(BaseMiddleware):
    """
    Middleware for preventing spam in Telegram bot.
    Limits the number of messages a user can send in a given time window.
    """

    def __init__(self, limit: int = SPAM_LIMIT, window: int = SPAM_TIME_WINDOW):
        self.limit = limit
        self.window = window
        super().__init__()

    async def __call__(self, handler, event, data):
        """Process the event through anti-spam filter."""
        # Only process messages and callback queries
        if isinstance(event, (Message, CallbackQuery)):
            user_id = event.from_user.id if event.from_user else None

            if user_id is None:
                return await handler(event, data)

            # Check if user is blocked and auto-unblock if time expired
            if user_id in blocked_users:
                block_time = blocked_users[user_id]
                if time.time() - block_time >= SPAM_BLOCK_DURATION:
                    # Unblock user
                    del blocked_users[user_id]
                    violation_counts[user_id] = 0
                    user_message_timestamps[user_id] = []
                    logger.info(f"User {user_id} has been auto-unblocked")
                else:
                    await self._notify_blocked(event, user_id)
                    return

            # Clean old timestamps
            current_time = time.time()
            user_message_timestamps[user_id] = [
                ts for ts in user_message_timestamps[user_id]
                if current_time - ts < self.window
            ]

            # Check message rate
            if len(user_message_timestamps[user_id]) >= self.limit:
                # User exceeded the limit
                violation_counts[user_id] += 1

                logger.warning(f"Spam detected from user {user_id}. Violation count: {violation_counts[user_id]}")

                # Auto-block if threshold exceeded
                if violation_counts[user_id] >= AUTO_BLOCK_THRESHOLD:
                    blocked_users[user_id] = time.time()
                    await self._block_user(user_id)
                    await self._notify_blocked(event, user_id)
                else:
                    # Just warn the user
                    await self._warn_user(event)

                return  # Don't process the message

            # Add current timestamp
            user_message_timestamps[user_id].append(current_time)

        return await handler(event, data)

    async def _warn_user(self, event):
        """Send a warning to the user about rate limiting."""
        try:
            if isinstance(event, Message):
                await event.answer(
                    f"<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> Juda ko‘p xabar yuboryapsiz! Iltimos, xabarlar orasida {self.window} soniya kuting.\n"
                    f"Qoidani yana buzsangiz, bloklanasiz.",
                    show_alert=True, parse_mode='HTML'
                )
            elif isinstance(event, CallbackQuery):
                await event.answer(
                    f"<tg-emoji emoji-id=\"5447644880824181073\">⚠️</tg-emoji> Juda ko‘p so‘rov yuboryapsiz! {self.window} soniya kuting.",
                    show_alert=True, parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Error sending warning: {e}")

    async def _notify_blocked(self, event, user_id):
        """Notify user that they are blocked."""
        try:
            block_time = SPAM_BLOCK_DURATION // 60
            if isinstance(event, Message):
                await event.answer(
                    f"<tg-emoji emoji-id=\"5240241223632954241\">🚫</tg-emoji> Spam uchun siz {block_time} daqiqaga bloklandingiz.\n"
                    f"Agar bu xatolik bo‘lsa, administratorga murojaat qiling.",
                    show_alert=True, parse_mode='HTML'
                )
            elif isinstance(event, CallbackQuery):
                await event.answer(
                    f"<tg-emoji emoji-id=\"5240241223632954241\">🚫</tg-emoji> Spam uchun siz {block_time} daqiqaga bloklandingiz.",
                    show_alert=True, parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Error notifying blocked user: {e}")

    async def _block_user(self, user_id):
        """Block user temporarily in memory (not in database)."""
        # Note: We don't block in database for auto-blocks, only in memory
        # This allows automatic unblock after the duration expires
        logger.info(f"User {user_id} has been temporarily blocked for spam (in-memory)")


# Function to unblock user after block duration
async def check_and_unblock_users():
    """Background task to check and unblock users when block duration expires."""
    while True:
        try:
            current_time = time.time()
            # This would require storing block times - simplified version
            # In production, you'd store block timestamps in a dict
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Error in unblock check: {e}")
            await asyncio.sleep(60)


# ==================== FLASK RATE LIMITING ====================
def rate_limit_ip(limit: int = FLASK_RATE_LIMIT, window: int = FLASK_RATE_WINDOW):
    """
    Rate limiting decorator for Flask routes.
    Prevents DDoS attacks by limiting requests from a single IP.
    """
    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            # Get client IP
            client_ip = request.remote_addr

            # Check if IP is blocked and auto-unblock if expired
            if client_ip in blocked_ips:
                block_time = blocked_ips[client_ip]
                if time.time() - block_time >= FLASK_DDOS_BLOCK_DURATION:
                    # Unblock IP
                    del blocked_ips[client_ip]
                    ip_request_timestamps[client_ip] = []
                    logger.info(f"IP {client_ip} has been auto-unblocked")
                else:
                    return jsonify({
                        "error": "Juda ko‘p so‘rov yubordingiz. Siz 5 daqiqaga bloklandingiz.",
                        "blocked": True
                    }), 429

            # Clean old timestamps
            current_time = time.time()
            ip_request_timestamps[client_ip] = [
                ts for ts in ip_request_timestamps[client_ip]
                if current_time - ts < window
            ]

            # Check request rate
            if len(ip_request_timestamps[client_ip]) >= limit:
                # Block the IP
                blocked_ips[client_ip] = time.time()
                logger.warning(f"DDoS attempt detected from IP: {client_ip}")

                return jsonify({
                    "error": "Juda ko‘p so‘rov yubordingiz. Iltimos, kuting.",
                    "rate_limit_exceeded": True
                }), 429

            # Add current timestamp
            ip_request_timestamps[client_ip].append(current_time)

            return f(*args, **kwargs)
        return decorated_function
    return decorator



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

_active_channels_cache, _cache_timestamp, CACHE_DURATION = None, None, 60

def get_db():
    conn = sqlite3.connect('bot.db', timeout=60)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.row_factory = sqlite3.Row
    return conn

def get_all_active_channels(force_refresh=False):
    global _active_channels_cache, _cache_timestamp
    current_time = datetime.now()
    if force_refresh or _active_channels_cache is None or _cache_timestamp is None or (current_time - _cache_timestamp).total_seconds() > CACHE_DURATION:
        channels = dict(REQUIRED_CHANNELS)
        try:
            with get_db() as db:
                if db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sponsors'").fetchone():
                    for sponsor in db.execute("SELECT channel_name, channel_id FROM sponsors WHERE is_active = TRUE").fetchall():
                        channels[sponsor['channel_name']] = sponsor['channel_id']
        except Exception as e:
            logger.error(f"Error fetching sponsors: {e}")
        _active_channels_cache, _cache_timestamp = channels, current_time
    return _active_channels_cache

def init_db():
    with get_db() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, is_subscribed BOOLEAN DEFAULT FALSE, last_sub_check DATETIME DEFAULT CURRENT_TIMESTAMP, first_name TEXT, last_name TEXT, class_name TEXT, is_registered BOOLEAN DEFAULT FALSE, rating INTEGER DEFAULT 0, photo_url TEXT, is_blocked BOOLEAN DEFAULT FALSE, server_nick TEXT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS user_tasks (user_id INTEGER, task_id INTEGER, is_correct BOOLEAN, earned_rating INTEGER, completed_at DATETIME DEFAULT CURRENT_TIMESTAMP, answers TEXT, correct_count INTEGER DEFAULT 0, incorrect_count INTEGER DEFAULT 0, started_at DATETIME, PRIMARY KEY (user_id, task_id))''')
        db.execute('''CREATE TABLE IF NOT EXISTS sponsors (id INTEGER PRIMARY KEY AUTOINCREMENT, channel_name TEXT NOT NULL, channel_id TEXT NOT NULL, is_active BOOLEAN DEFAULT TRUE)''')
        db.execute('''CREATE TABLE IF NOT EXISTS promos (code TEXT PRIMARY KEY, discount_percent INTEGER, category TEXT, is_one_time BOOLEAN)''')
        db.execute('''CREATE TABLE IF NOT EXISTS reviews (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, username TEXT, stars INTEGER, text TEXT, review_time DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        db.execute('''CREATE TABLE IF NOT EXISTS purchases (purchase_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, item_id INTEGER, item_name TEXT, price INTEGER, status TEXT DEFAULT 'pending', created_at DATETIME DEFAULT CURRENT_TIMESTAMP, server_nick TEXT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS task_bundles (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, league_id TEXT DEFAULT 'all', time_limit INTEGER DEFAULT 0, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')
        db.execute('''CREATE TABLE IF NOT EXISTS bundle_questions (id INTEGER PRIMARY KEY AUTOINCREMENT, bundle_id INTEGER NOT NULL, question TEXT NOT NULL, options TEXT NOT NULL, correct_option INTEGER NOT NULL DEFAULT 0, rating INTEGER DEFAULT 5, FOREIGN KEY (bundle_id) REFERENCES task_bundles(id) ON DELETE CASCADE)''')
        db.execute('''CREATE TABLE IF NOT EXISTS system_settings (key TEXT PRIMARY KEY, value TEXT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS items (item_id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT, description TEXT, options TEXT, correct_option INTEGER)''')

        for col in db.execute("PRAGMA table_info(users)").fetchall():
            col_name = col[1]
            if col_name == 'photo_url' and not db.execute("SELECT photo_url FROM users WHERE user_id = -1").fetchone():
                pass

        items_data = [
            (1, 'Что означает этот знак + ?', 10, 'all', 'Математическая задача', 'знак принадлежности|пересечение|объединение|пустое множество', 2),
            (2, 'Сколько будет 2 + 2 * 2?', 5, 'all', 'Математика', '4|6|8|0', 1),
            (3, 'Столица Франции?', 5, 'all', 'География', 'Берлин|Лондон|пАРИЖ|Рим', 2),
            (4, 'Самая большая планета?', 5, 'all', 'Астрономия', 'Марс|Земля|Юпитер Сатурн', 2),
            (5, 'Химический символ золота?', 5, 'all', 'Химия', 'Ag|Au|Fe|Cu', 1)
        ]
        for item in items_data:
            db.execute("INSERT OR REPLACE INTO items (item_id, name, price, category, description, options, correct_option) VALUES (?, ?, ?, ?, ?, ?, ?)", item)

        if not db.execute("SELECT value FROM system_settings WHERE key = 'season_start'").fetchone():
            db.execute("INSERT INTO system_settings (key, value) VALUES ('season_start', ?)", (datetime.now().isoformat(),))

        db.commit()
        github_backup.auto_push()

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
    return user[key] if user and key in user.keys() else default

@app.route('/api/user/<int:user_id>')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def get_user_api(user_id):
    try:
        username = request.args.get('username', f"User {user_id}")
        name = request.args.get('name', username)
        photo_param = request.args.get('photo')

        with get_db() as db:
            user = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if not user:
                db.execute("INSERT OR IGNORE INTO users (user_id, username, photo_url) VALUES (?, ?, ?)", (user_id, username, photo_param))
                db.commit()
                github_backup.auto_push()
                user = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

            photo_url = get_user_row(user, 'photo_url')
            if photo_param and photo_param != photo_url:
                db.execute("UPDATE users SET photo_url = ? WHERE user_id = ?", (photo_param, user_id))
                db.commit()
                github_backup.auto_push()
                photo_url = photo_param

            rating = get_user_row(user, 'rating', 0)
            display_name = name or username

            if user:
                f_name = get_user_row(user, 'first_name')
                l_name = get_user_row(user, 'last_name')
                if f_name and l_name:
                    display_name = f"{f_name} {l_name}"
                elif f_name:
                    display_name = f_name

            class_name = get_user_row(user, 'class_name')
            league, league_place, league_rank, top_players = "Boshlang'ich liga", "Hammasi", None, []

            if class_name:
                clean_class = ''.join(filter(str.isdigit, class_name))
                if clean_class:
                    class_num = int(clean_class)
                    if 1 <= class_num <= 4:
                        league, league_place = "Boshlang'ich liga", "1-4 sinf"
                        league_classes = ['1', '2', '3', '4', '1 класс', '2 класс', '3 класс', '4 класс']
                    elif 5 <= class_num <= 6:
                        league, league_place = "Bronza liga", "5-6 sinf"
                        league_classes = ['5', '6', '5 класс', '6 класс']
                    elif 7 <= class_num <= 8:
                        league, league_place = "Kumush liga", "7-8 sinf"
                        league_classes = ['7', '8', '7 класс', '8 класс']
                    elif 9 <= class_num <= 11:
                        league, league_place = "Oltin liga", "9-11 sinf"
                        league_classes = ['9', '10', '11', '9 класс', '10 класс', '11 класс']
                    else:
                        league_classes = []
                else:
                    league_classes = []
            else:
                league_classes = []

            if not league_classes:
                league, league_place = "Umumiy liga", "Barcha sinflar"

            try:
                user_rating_value = rating
                if league_classes:
                    placeholders = ','.join(['?' for _ in league_classes])
                    clean_league_nums = [''.join(filter(str.isdigit, c)) for c in league_classes if ''.join(filter(str.isdigit, c))]
                    count_query = f"SELECT COUNT(*) as cnt FROM users WHERE (class_name IN ({placeholders})"
                    if clean_league_nums:
                        count_query += f" OR {' OR '.join(['class_name LIKE ?' for _ in range(len(clean_league_nums)*2)])}"
                    count_query += ") AND (rating > ? OR (rating = ? AND user_id < ?))"
                    count_params = list(league_classes) + [f"{n}%" for n in clean_league_nums] + [f"%{n}" for n in clean_league_nums] + [user_rating_value, user_rating_value, user_id]
                    higher_count = db.execute(count_query, count_params).fetchone()
                    league_rank = (higher_count['cnt'] or 0) + 1

                    query = f"SELECT user_id, rating, first_name, last_name, username, photo_url FROM users WHERE (class_name IN ({placeholders})"
                    if clean_league_nums:
                        query += f" OR {' OR '.join(['class_name LIKE ?' for _ in range(len(clean_league_nums)*2)])}"
                    query += ") ORDER BY rating DESC, user_id ASC LIMIT 3"
                    league_top = db.execute(query, count_params[:-3]).fetchall()
                    for idx, lu in enumerate(league_top, 1):
                        p_name = f"{lu['first_name']} {lu['last_name']}" if lu['first_name'] else (lu['username'] or f"User {lu['user_id']}")
                        top_players.append({"user_id": lu['user_id'], "name": p_name, "rating": lu['rating'], "rank": idx, "photo": get_user_row(lu, 'photo_url')})
                else:
                    higher_count = db.execute("SELECT COUNT(*) as cnt FROM users WHERE (rating > ? OR (rating = ? AND user_id < ?))", (user_rating_value, user_rating_value, user_id)).fetchone()
                    league_rank = (higher_count['cnt'] or 0) + 1
                    all_users = db.execute("SELECT user_id, rating, first_name, last_name, username, photo_url FROM users ORDER BY rating DESC, user_id ASC LIMIT 3").fetchall()
                    for idx, lu in enumerate(all_users, 1):
                        p_name = f"{lu['first_name']} {lu['last_name']}" if lu['first_name'] else (lu['username'] or f"User {lu['user_id']}")
                        top_players.append({"user_id": lu['user_id'], "name": p_name, "rating": lu['rating'], "rank": idx, "photo": get_user_row(lu, 'photo_url')})

                all_leagues_tops = {}
                leagues_config = {"Бронзовая лига": ['5', '6', '5 класс', '6 класс'], "Серебряная лига": ['7', '8', '7 класс', '8 класс'], "Золотая лига": ['9', '10', '11', '9 класс', '10 класс', '11 класс']}
                for l_name, l_classes in leagues_config.items():
                    placeholders = ','.join(['?' for _ in l_classes])
                    clean_league_nums = [''.join(filter(str.isdigit, c)) for c in l_classes if ''.join(filter(str.isdigit, c))]
                    query = f"SELECT user_id, rating, first_name, last_name, username, photo_url FROM users WHERE (class_name IN ({placeholders})"
                    if clean_league_nums:
                        query += f" OR {' OR '.join(['class_name LIKE ?' for _ in range(len(clean_league_nums)*2)])}"
                    query += ") ORDER BY rating DESC LIMIT 3"
                    params = list(l_classes) + [f"{n}%" for n in clean_league_nums] + [f"%{n}" for n in clean_league_nums]
                    tops = db.execute(query, params).fetchall()
                    all_leagues_tops[l_name] = [{"user_id": t['user_id'], "name": f"{t['first_name']} {t['last_name']}" if t['first_name'] else (t['username'] or f"User {t['user_id']}"), "rating": t['rating'], "photo": get_user_row(t, 'photo_url')} for t in tops]
            except Exception as e:
                logger.error(f"Error fetching top players: {e}")
                all_leagues_tops = {}

            season_days = 30
            season_start_val = db.execute("SELECT value FROM system_settings WHERE key = 'season_start'").fetchone()
            if season_start_val:
                try:
                    start_dt = datetime.fromisoformat(season_start_val['value'])
                    season_days = max(0, 30 - (datetime.now() - start_dt).days)
                except:
                    pass

            return jsonify({
                "user_id": user_id,
                "username": get_user_row(user, 'username') or username,
                "first_name": get_user_row(user, 'first_name'),
                "last_name": get_user_row(user, 'last_name'),
                "class_name": class_name,
                "is_registered": bool(get_user_row(user, 'is_registered', False)),
                "rating": rating,
                "referrals": rating,
                "status": "Boshlang'ich" if rating < 50 else ("O'rganuvchi" if rating < 100 else "Master"),
                "league": league,
                "league_place": league_place,
                "league_rank": league_rank,
                "display_name": display_name,
                "photo": photo_url,
                "is_admin": str(user_id) in os.environ.get("ADMIN_IDS", "7592032451").split(','),
                "server_nick": get_user_row(user, 'server_nick'),
                "top_players": top_players,
                "all_leagues_tops": all_leagues_tops,
                "days_left": season_days,
                "is_blocked": bool(get_user_row(user, 'is_blocked', False))
            })
    except Exception as e:
        logger.error(f"Error in get_user_api: {e}")
        return jsonify({"message": str(e)}), 500

@app.route('/api/register', methods=['POST'])
@rate_limit_ip(limit=20, window=60)  # Stricter limit for registration
def register_user():
    try:
        data = request.json
        user_id, first_name, last_name, class_name = data.get('user_id'), data.get('first_name'), data.get('last_name'), data.get('class_name')
        if not all([user_id, first_name, last_name, class_name]):
            return jsonify({"success": False, "message": "Missing data"}), 400
        with get_db() as db:
            db.execute("UPDATE users SET first_name = ?, last_name = ?, class_name = ?, is_registered = TRUE WHERE user_id = ?", (first_name, last_name, class_name, user_id))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

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

        with get_db() as db:
            if not db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)).fetchone():
                return jsonify({"success": False, "message": "User not found"}), 404

            # Validate score - get max possible rating for this bundle
            bundle_id = None
            if task_id and task_id >= 1000:
                bundle_id = task_id - 1000
            elif task_id:
                bundle_id = task_id

            max_rating = 0
            if bundle_id:
                max_rating = db.execute("SELECT COALESCE(SUM(rating), 0) as total FROM bundle_questions WHERE bundle_id = ?", (bundle_id,)).fetchone()
                max_rating = max_rating['total'] if max_rating else 0

            # Also check items table for single questions
            if not max_rating and task_id and task_id < 1000:
                item = db.execute("SELECT price FROM items WHERE item_id = ?", (task_id,)).fetchone()
                if item:
                    max_rating = item['price']

            # Cap the score to maximum possible rating
            if score > max_rating:
                logger.warning(f"User {user_id} tried to submit score {score} but max is {max_rating}. Capping.")
                score = max_rating if max_rating > 0 else 0

            # Ensure score is not negative
            if score < 0:
                score = 0

            db.execute("UPDATE users SET rating = rating + ? WHERE user_id = ?", (score, user_id))
            if task_id:
                db.execute("INSERT OR REPLACE INTO user_tasks (user_id, task_id, is_correct, earned_rating, answers, correct_count, incorrect_count) VALUES (?, ?, ?, ?, ?, ?, ?)", (user_id, task_id, is_correct, score, json.dumps(answers), correct_count, incorrect_count))
            db.commit()
            github_backup.auto_push()
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

        with get_db() as db:
            existing = db.execute("SELECT started_at FROM user_tasks WHERE user_id = ? AND task_id = ?", (user_id, task_id)).fetchone()
            if existing and existing['started_at']:
                return jsonify({"success": True, "started_at": existing['started_at'], "message": "Already started"})
            now = datetime.now().isoformat()
            db.execute("INSERT OR REPLACE INTO user_tasks (user_id, task_id, started_at, completed_at) VALUES (?, ?, ?, ?)", (user_id, task_id, now, now))
            db.commit()
            github_backup.auto_push()
            return jsonify({"success": True, "started_at": now})
    except Exception as e:
        logger.error(f"Error starting bundle: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/bundle/check-time', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def check_bundle_time():
    try:
        data = request.json
        user_id, task_id = data.get('user_id'), data.get('task_id')
        if not user_id or not task_id:
            return jsonify({"success": False, "message": "Missing user_id or task_id"}), 400

        with get_db() as db:
            user_task = db.execute("SELECT ut.started_at, tb.time_limit FROM user_tasks ut JOIN bundle_questions bq ON ? - 1000 = bq.bundle_id JOIN task_bundles tb ON bq.bundle_id = tb.id WHERE ut.user_id = ? AND ut.task_id = ?", (task_id, user_id, task_id)).fetchone()
            task = db.execute("SELECT is_correct FROM user_tasks WHERE user_id = ? AND task_id = ?", (user_id, task_id)).fetchone()
            if task and task['is_correct'] is not None:
                return jsonify({"success": True, "expired": True, "message": "Bundle already completed"})
            if not user_task or not user_task['started_at']:
                return jsonify({"success": True, "expired": False, "started": False})

            bundle_id = task_id - 1000
            bundle = db.execute("SELECT time_limit FROM task_bundles WHERE id = ?", (bundle_id,)).fetchone()
            time_limit = bundle['time_limit'] if bundle else 0

            if time_limit <= 0:
                return jsonify({"success": True, "expired": False, "time_limit": 0})

            started_at = datetime.fromisoformat(user_task['started_at'])
            elapsed = (datetime.now() - started_at).total_seconds()
            remaining = time_limit * 60 - elapsed

            if remaining <= 0:
                return jsonify({"success": True, "expired": True, "remaining_seconds": 0})

            return jsonify({"success": True, "expired": False, "started_at": user_task['started_at'], "time_limit": time_limit, "remaining_seconds": int(remaining)})
    except Exception as e:
        logger.error(f"Error checking bundle time: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/user/<int:user_id>/tasks')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def get_user_tasks(user_id):
    try:
        with get_db() as db:
            user = db.execute("SELECT class_name FROM users WHERE user_id = ?", (user_id,)).fetchone()
            user_class = user['class_name'] if user else None
            user_league = None
            if user_class:
                import re
                match = re.search(r'(\d+)', user_class)
                if match:
                    class_num = int(match.group(1))
                    if 5 <= class_num <= 6: user_league = 'bronza'
                    elif 7 <= class_num <= 8: user_league = 'kumush'
                    elif 9 <= class_num <= 11: user_league = 'oltin'

            all_bundles = db.execute("SELECT * FROM task_bundles ORDER BY created_at DESC").fetchall()
            completed_rows = db.execute("SELECT task_id FROM user_tasks WHERE user_id = ?", (user_id,)).fetchall()
            completed_ids = [row['task_id'] for row in completed_rows]

            active_list = []
            for b in all_bundles:
                bundle_id = b['id'] + 1000
                bundle_league = b['league_id'] if 'league_id' in b.keys() else None
                if bundle_league and user_league and bundle_league != user_league:
                    continue
                if bundle_id not in completed_ids:
                    questions = db.execute("SELECT * FROM bundle_questions WHERE bundle_id = ?", (b['id'],)).fetchall()
                    if questions:
                        questions_list = [{"id": q['id'], "question": q['question'], "options": q['options'], "correct_option": q['correct_option'], "rating": q['rating'] or 5} for q in questions]
                        total_rating = sum(q['rating'] or 5 for q in questions)
                        time_limit = b['time_limit'] if 'time_limit' in b.keys() else 0
                        active_list.append({"item_id": bundle_id, "name": b['name'], "category": "bundle", "questions": questions_list, "price": total_rating, "total_questions": len(questions_list), "time_limit": time_limit})

            completed_list = []
            completed_details = db.execute("SELECT * FROM user_tasks WHERE user_id = ? ORDER BY completed_at DESC", (user_id,)).fetchall()
            for row in completed_details:
                task_id = row['task_id']
                if task_id >= 1000:
                    bundle_db_id = task_id - 1000
                    bundle = db.execute("SELECT name FROM task_bundles WHERE id = ?", (bundle_db_id,)).fetchone()
                    if bundle:
                        completed_list.append({"task_id": task_id, "name": bundle['name'], "earned_rating": row['earned_rating'] or 0, "correct_count": row['correct_count'] or 0, "incorrect_count": row['incorrect_count'] or 0, "answers": row['answers'] or '{}', "is_correct": bool(row['is_correct']) if row['is_correct'] is not None else False})

            return jsonify({"active": active_list, "completed": completed_list})
    except Exception as e:
        logger.error(f"Error getting user tasks: {e}")
        return jsonify({"active": [], "completed": []}), 500

@app.route('/api/user/<int:user_id>/purchases')
def get_user_purchases(user_id):
    return jsonify({"purchases": []})

@app.route('/api/items')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def get_items():
    try:
        with get_db() as db:
            items = db.execute("SELECT * FROM items").fetchall()
            return jsonify([{"item_id": item['item_id'], "name": item['name'], "price": item['price'], "category": item['category'], "description": item['description'], "options": item['options'], "correct_option": item['correct_option']} for item in items])
    except Exception as e:
        logger.error(f"Error getting items: {e}")
        return jsonify([]), 500

@app.route('/api/user/nickname', methods=['POST'])
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def set_nickname():
    try:
        data = request.json
        with get_db() as db:
            db.execute("UPDATE users SET server_nick = ? WHERE user_id = ?", (data.get('nickname'), data.get('user_id')))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

def check_admin_pass(pass_key):
    return pass_key == os.environ.get("ADMIN_PASS", "admin123")

@app.route('/api/admin/bundles')
def get_bundles():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify([]), 403
    try:
        with get_db() as db:
            bundles = db.execute("SELECT * FROM task_bundles ORDER BY created_at DESC").fetchall()
            result = []
            for b in bundles:
                questions = db.execute("SELECT * FROM bundle_questions WHERE bundle_id = ?", (b['id'],)).fetchall()
                questions_list = [{"id": q['id'], "question": q['question'], "options": q['options'], "correct_option": q['correct_option'], "rating": q['rating']} for q in questions]
                result.append({"id": b['id'], "name": b['name'], "league_id": get_user_row(b, 'league_id', 'all'), "time_limit": get_user_row(b, 'time_limit', 0), "questions": questions_list, "created_at": b['created_at']})
            return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting bundles: {e}")
        return jsonify([]), 500

@app.route('/api/admin/bundle', methods=['POST'])
def add_bundle():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        data = request.json
        name, questions = data.get('name', '').strip(), data.get('questions', [])
        league_id, time_limit = data.get('league_id', 'all'), data.get('time_limit', 0)
        if not name:
            return jsonify({"success": False, "message": "Название сборки обязательно"}), 400
        if not questions:
            return jsonify({"success": False, "message": "Добавьте хотя бы один вопрос"}), 400

        with get_db() as db:
            cursor = db.execute("INSERT INTO task_bundles (name, league_id, time_limit) VALUES (?, ?, ?)", (name, league_id, time_limit))
            bundle_id = cursor.lastrowid
            for q in questions:
                if not q.get('question') or not q.get('options'):
                    continue
                db.execute("INSERT INTO bundle_questions (bundle_id, question, options, correct_option, rating) VALUES (?, ?, ?, ?, ?)", (bundle_id, q.get('question', '').strip(), q.get('options', '').strip(), int(q.get('correct_option', 0)), int(q.get('rating', 5))))
            db.commit()
            github_backup.auto_push()
            return jsonify({"success": True, "bundle_id": bundle_id})
    except Exception as e:
        logger.error(f"Error adding bundle: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/bundle/<int:bundle_id>', methods=['DELETE'])
def delete_bundle(bundle_id):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        with get_db() as db:
            if not db.execute("SELECT id FROM task_bundles WHERE id = ?", (bundle_id,)).fetchone():
                return jsonify({"success": False, "message": "Сборка не найдена"}), 404
            db.execute("DELETE FROM user_tasks WHERE task_id = ?", (bundle_id + 1000,))
            db.execute("DELETE FROM bundle_questions WHERE bundle_id = ?", (bundle_id,))
            db.execute("DELETE FROM task_bundles WHERE id = ?", (bundle_id,))
            db.commit()
            github_backup.auto_push()
            return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error deleting bundle: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/bundle/<int:bundle_id>', methods=['PUT'])
def edit_bundle(bundle_id):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        data = request.json
        name, questions = data.get('name', '').strip(), data.get('questions', [])
        league_id, time_limit = data.get('league_id', 'all'), data.get('time_limit', 0)
        if not name or not questions:
            return jsonify({"success": False, "message": "Название и вопросы обязательны"}), 400

        with get_db() as db:
            if not db.execute("SELECT id FROM task_bundles WHERE id = ?", (bundle_id,)).fetchone():
                return jsonify({"success": False, "message": "Сборка не найдена"}), 404
            db.execute("UPDATE task_bundles SET name = ?, league_id = ?, time_limit = ? WHERE id = ?", (name, league_id, time_limit, bundle_id))
            db.execute("DELETE FROM bundle_questions WHERE bundle_id = ?", (bundle_id,))
            for q in questions:
                if not q.get('question') or not q.get('options'):
                    continue
                db.execute("INSERT INTO bundle_questions (bundle_id, question, options, correct_option, rating) VALUES (?, ?, ?, ?, ?)", (bundle_id, q['question'].strip(), q['options'].strip(), int(q.get('correct_option', 0)), int(q.get('rating', 5))))
            db.commit()
            github_backup.auto_push()
            return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error editing bundle: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/stats')
@rate_limit_ip(limit=FLASK_RATE_LIMIT, window=FLASK_RATE_WINDOW)
def admin_stats():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({}), 403
    try:
        with get_db() as db:
            total_users = db.execute("SELECT COUNT(*) as cnt FROM users").fetchone()['cnt']
            registered_users = db.execute("SELECT COUNT(*) as cnt FROM users WHERE is_registered = TRUE").fetchone()['cnt']
            total_tasks = db.execute("SELECT COUNT(*) as cnt FROM user_tasks").fetchone()['cnt']
            total_rating = db.execute("SELECT COALESCE(SUM(rating), 0) as total FROM users").fetchone()['total']
            avg_rating = db.execute("SELECT COALESCE(AVG(rating), 0) as avg FROM users WHERE rating > 0").fetchone()['avg']
            today = datetime.now().strftime('%Y-%m-%d')
            new_today = db.execute("SELECT COUNT(*) as cnt FROM users WHERE date(last_sub_check) = ?", (today,)).fetchone()['cnt']
            bronze = db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('5', '6', '5 класс', '6 класс') OR class_name LIKE '5%' OR class_name LIKE '6%'").fetchone()['cnt']
            silver = db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('7', '8', '7 класс', '8 класс') OR class_name LIKE '7%' OR class_name LIKE '8%'").fetchone()['cnt']
            gold = db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('9', '10', '11', '9 класс', '10 класс', '11 класс') OR class_name LIKE '9%' OR class_name LIKE '10%' OR class_name LIKE '11%'").fetchone()['cnt']
            top_users = db.execute("SELECT user_id, username, first_name, last_name, rating, class_name FROM users ORDER BY rating DESC LIMIT 10").fetchall()

            season_start_val = db.execute("SELECT value FROM system_settings WHERE key = 'season_start'").fetchone()
            days_left = 30
            if season_start_val:
                try:
                    start_dt = datetime.fromisoformat(season_start_val['value'])
                    days_left = max(0, 30 - (datetime.now() - start_dt).days)
                except:
                    pass

            return jsonify({"total_users": total_users, "registered_users": registered_users, "total_tasks_completed": total_tasks, "total_rating": total_rating, "avg_rating": round(avg_rating, 1) if avg_rating else 0, "new_today": new_today, "days_left": days_left, "bronze_league": bronze, "silver_league": silver, "gold_league": gold, "top_users": [dict(u) for u in top_users]})
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({}), 500

@app.route('/api/admin/orders')
def admin_orders():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify([]), 403
    try:
        with get_db() as db:
            orders = db.execute("SELECT * FROM purchases WHERE status = 'pending'").fetchall()
            return jsonify([dict(o) for o in orders])
    except:
        return jsonify([]), 500

@app.route('/api/admin/reviews')
def admin_reviews():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify([]), 403
    try:
        with get_db() as db:
            reviews = db.execute("SELECT * FROM reviews ORDER BY review_time DESC").fetchall()
            return jsonify([dict(r) for r in reviews])
    except:
        return jsonify([]), 500

@app.route('/api/admin/promos')
def admin_promos():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify([]), 403
    try:
        with get_db() as db:
            promos = db.execute("SELECT * FROM promos").fetchall()
            return jsonify([dict(p) for p in promos])
    except:
        return jsonify([]), 500

@app.route('/api/admin/promo/create', methods=['POST'])
def create_promo():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        data = request.json
        with get_db() as db:
            db.execute("INSERT INTO promos (code, discount_percent, category, is_one_time) VALUES (?, ?, ?, ?)", (data['code'], data['discount'], data['category'], data['is_one_time']))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True})
    except:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/promo/delete/<code>', methods=['POST'])
def delete_promo(code):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        with get_db() as db:
            db.execute("DELETE FROM promos WHERE code = ?", (code,))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True})
    except:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/promo/stats/<code>')
def promo_stats(code):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"total": 0, "month": 0, "day": 0}), 403
    return jsonify({"total": 0, "month": 0, "day": 0})

@app.route('/api/admin/order/<int:order_id>/<action_type>', methods=['POST'])
def admin_order_action(order_id, action_type):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False}), 403
    try:
        status = 'completed' if action_type == 'confirm' else 'rejected'
        with get_db() as db:
            db.execute("UPDATE purchases SET status = ? WHERE purchase_id = ?", (status, order_id))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True})
    except:
        return jsonify({"success": False}), 500

@app.route('/api/admin/users')
def admin_get_users():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify([]), 403
    try:
        with get_db() as db:
            users = db.execute("SELECT * FROM users ORDER BY rating DESC").fetchall()
            return jsonify([dict(u) for u in users])
    except Exception as e:
        logger.error(f"Error in admin_get_users: {e}")
        return jsonify([]), 500

@app.route('/api/admin/user/<int:user_id>/delete', methods=['POST'])
def admin_delete_user(user_id):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        with get_db() as db:
            db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            db.execute("DELETE FROM user_tasks WHERE user_id = ?", (user_id,))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/user/<int:user_id>/update', methods=['POST'])
def admin_update_user(user_id):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        data = request.json
        with get_db() as db:
            db.execute("UPDATE users SET first_name = ?, last_name = ?, class_name = ?, rating = ?, username = ? WHERE user_id = ?", (data.get('first_name'), data.get('last_name'), data.get('class_name'), data.get('rating'), data.get('username'), user_id))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error updating user: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/user/<int:user_id>/block', methods=['POST'])
def admin_block_user(user_id):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        action = request.args.get('action', 'block')
        blocked = action == 'block'
        with get_db() as db:
            db.execute("UPDATE users SET is_blocked = ? WHERE user_id = ?", (blocked, user_id))
            db.commit()
            github_backup.auto_push()
        return jsonify({"success": True, "is_blocked": blocked})
    except Exception as e:
        logger.error(f"Error blocking user: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/user/<int:user_id>/status')
def admin_user_status(user_id):
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Unauthorized"}), 403
    try:
        with get_db() as db:
            user = db.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if user:
                return jsonify({"success": True, "is_blocked": bool(user['is_blocked'])})
            return jsonify({"success": False, "message": "User not found"}), 404
    except Exception as e:
        logger.error(f"Error getting user status: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/admin/reset-season', methods=['POST'])
def reset_season():
    if not check_admin_pass(request.args.get('pass')):
        return jsonify({"success": False, "message": "Access denied"}), 403
    try:
        with get_db() as db:
            db.execute("UPDATE users SET rating = 0")
            db.execute("DELETE FROM user_tasks")
            db.execute("UPDATE system_settings SET value = ? WHERE key = 'season_start'", (datetime.now().isoformat(),))
            db.commit()
            github_backup.auto_push()
        logger.info("Season reset successful")
        return jsonify({"success": True, "message": "Mavsumni muvaffaqiyatli qayta boshlash"})
    except Exception as e:
        logger.error(f"Error in reset_season: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

def run_flask():
    app.run(host='0.0.0.0', port=5000)

# Telegram Bot
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Register anti-spam middleware
spam_middleware = AntiSpamMiddleware(limit=SPAM_LIMIT, window=SPAM_TIME_WINDOW)
dp.message.middleware.register(spam_middleware)
dp.callback_query.middleware.register(spam_middleware)

router = Router()

class AdminState(StatesGroup):
    waiting_broadcast_text = State()
    waiting_bundle_name = State()
    waiting_bundle_question = State()
    waiting_block_user_id = State()
    waiting_unblock_user_id = State()

def main_menu_keyboard(user_id=None, name=None, photo_url=None):
    import time
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

async def get_profile_photo(user_id):
    photo_url = None
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file = await bot.get_file(photos.photos[0][-1].file_id)
            photo_url = f"https://api.telegram.org/file/bot{TOKEN}/{file.file_path}" if file.file_path and not file.file_path.startswith('http') else file.file_path
            with get_db() as db:
                db.execute("UPDATE users SET photo_url = ? WHERE user_id = ?", (photo_url, user_id))
                db.commit()
                github_backup.auto_push()
    except Exception as e:
        logger.error(f"Error getting user photo: {e}")
    return photo_url

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
    with get_db() as db:
        try:
            user_data = db.execute("SELECT is_subscribed, last_sub_check FROM users WHERE user_id = ?", (user_id,)).fetchone()
        except sqlite3.OperationalError:
            return False
        if not user_data:
            return False
        last_check = datetime.strptime(user_data['last_sub_check'], '%Y-%m-%d %H:%M:%S') if isinstance(user_data['last_sub_check'], str) else user_data['last_sub_check']
        if force_check or (datetime.now() - last_check).total_seconds() > 3600:
            is_subscribed = await check_subscription(user_id)
            db.execute("UPDATE users SET is_subscribed = ?, last_sub_check = CURRENT_TIMESTAMP WHERE user_id = ?", (is_subscribed, user_id))
            db.commit()
            github_backup.auto_push()
            return is_subscribed
        return user_data['is_subscribed']

@router.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    if not user:
        return
    logger.info(f"Command /start from user {user.id}")

    try:
        with get_db() as db:
            db.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user.id, user.username or ""))
            db.commit()
            github_backup.auto_push()

        with get_db() as db:
            db_user = db.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user.id,)).fetchone()
            if db_user and db_user['is_blocked']:
                await message.answer("<tg-emoji emoji-id=\"5260293700088511294\">⛔</tg-emoji> <b>Kirish taqiqlangan</b>\n\nSizning akkauntingiz bu botda bloklangan.\nBlokdan chiqarish uchun administratorga murojaat qiling: @s_narzimurodov\n\n<i>Murojaat uchun ID: {user.id}</i>", parse_mode='HTML')
                return

        is_subscribed = await verify_subscription(user.id, force_check=True)
        if not is_subscribed:
            await message.answer("<tg-emoji emoji-id=\"5424818078833715060\">📢</tg-emoji> <b>Botdan foydalanish uchun kanallarimizga obuna bo'ling:</b>", parse_mode='HTML', reply_markup=channels_keyboard())
            return

        photo_url = await get_profile_photo(user.id)
        reply_markup = main_menu_keyboard(user.id, user.first_name, photo_url)
        text = f"<tg-emoji emoji-id=\"5413694143601842851\">👋</tg-emoji> Salom, <b>{user.first_name}</b>! STIM quiz botiga xush kelibsiz! <tg-emoji emoji-id=\"5992459729975122233\">📱</tg-emoji>\n\n<tg-emoji emoji-id=\"5406745015365943482\">👇</tg-emoji> <b>Quyidagi tugmalardan foydalaning:</b>"

        try:
            if WELCOME_IMAGE_URL:
                await message.answer_photo(photo=WELCOME_IMAGE_URL, caption=text, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await message.answer(text=text, reply_markup=reply_markup, parse_mode='HTML')
        except:
            await message.answer(text=text, reply_markup=reply_markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Critical error in start command: {e}")
        try:
            await message.answer(f"Salom, {user.first_name}! Yuklashda xatolik yuz berdi, lekin menuni ochishingiz mumkin:", reply_markup=main_menu_keyboard(user.id, user.first_name, None))
        except:
            pass

@router.message(Command("help"))
async def cmd_help(message: Message):
    await cmd_start(message)

@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Sizda ushbu komandaga ruxsat yo'q.")
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Xabar jo'natish", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="📣 Yangi vazifalar haqida e'lon", callback_data="admin_announce")],
        [InlineKeyboardButton(text="📊 Statistika", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Foydalanuvchilar", callback_data="admin_users")],
        [InlineKeyboardButton(text="📝 To'plamlar", callback_data="admin_bundles")],
        [InlineKeyboardButton(text="🔄 Mavsumni qayta boshlash", callback_data="admin_reset_season")],
        [InlineKeyboardButton(text="🔒 Bloklash", callback_data="admin_block_ask")],
        [InlineKeyboardButton(text="🔓 Blokdan chiqarish", callback_data="admin_unblock_ask")],
        [InlineKeyboardButton(text="🔓 Panerni yopish", callback_data="admin_close")]
    ])
    await message.answer("Admin panelga xush kelibsiz:", reply_markup=keyboard)

@router.message(Command("db"))
async def cmd_db(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Sizda ushbu komandaga ruxsat yo'q.")
        return
    if not os.path.exists('bot.db'):
        await message.answer("❌ Bazaviy fayl topilmadi.")
        return
    try:
        await message.answer("📦 Bazaviy fayl jo'natilmoqda...")
        await message.answer_document(document=FSInputFile('bot.db', filename='bot.db'))
    except Exception as e:
        logger.error(f"Error sending database file: {e}")
        await message.answer(f"❌ Xatolik yuz berdi: {str(e)}")

@router.message(Command("backup"))
async def cmd_backup(message: Message):
    """Команда для создания резервной копии базы данных в GitHub"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("Sizda ushbu komandaga ruxsat yo'q.")
        return
    
    github_token = os.environ.get('GITHUB_TOKEN', '')
    github_repo = os.environ.get('GITHUB_REPO', '')
    
    if not github_token or not github_repo:
        await message.answer("❌ GitHub не настроен. Установите GITHUB_TOKEN и GITHUB_REPO")
        return
    
    try:
        github_backup.configure(
            github_token=github_token,
            github_repo=github_repo,
            github_branch=os.environ.get('GITHUB_BRANCH', 'main'),
            db_path='bot.db',
            backup_path=os.environ.get('BACKUP_PATH', 'backups')
        )
        await message.answer("⏳ Резервная копия загружается в GitHub...")
        
        if github_backup.push_db(message="Manual backup by admin"):
            await message.answer("✅ Резервная копия успешно создана!")
        else:
            await message.answer("❌ Ошибка при создании резервной копии")
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@router.message(Command("done"))
async def cmd_done(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get('state') != 'waiting_bundle_question':
        await message.answer("ℹ️ Faol to'plam yaratish jarayoni yo'q.")
        return
    bundle_name = data.get('bundle_name')
    questions = data.get('bundle_questions', [])
    if not questions:
        await message.answer("❌ Siz birorta savol qo'shmadingiz! To'plam bekor qilindi.")
        await state.clear()
        return
    try:
        with get_db() as db:
            cursor = db.execute("INSERT INTO task_bundles (name) VALUES (?)", (bundle_name,))
            bundle_id = cursor.lastrowid
            for q in questions:
                db.execute("INSERT INTO bundle_questions (bundle_id, question, options, correct_option, rating) VALUES (?, ?, ?, ?, ?)", (bundle_id, q['question'], q['options'], q['correct_option'], q['rating']))
            db.commit()
            github_backup.auto_push()
        await message.answer(f"✅ <b>To'plam yaratildi!</b>\n\n📦 Nomi: {bundle_name}\n📝 Savollar soni: {len(questions)}\n\nTo'plam endi Mini App'da mavjud.", parse_mode='HTML')
    except Exception as e:
        await message.answer(f"❌ To'plamni yaratishda xatolik yuz berdi: {str(e)}")
    await state.clear()

# Message handlers for ReplyKeyboard buttons
@router.message(F.text == "Bot haqida")
async def menu_about(message: Message):
    about_text = "<tg-emoji emoji-id=\"5334544901428229844\">ℹ️</tg-emoji> <b>Bot haqida</b>\n\nBu reyting tizimiga ega bo'lgan ta'limiy bot.\nVazifalarni bajaring, reytingni oshiring va boshqa ishtirokchilar bilan bellashing!\n\n<tg-emoji emoji-id=\"5231200819986047254\">📊</tg-emoji> <b>Funksiyalar:</b>\n• Turli toifadagi vazifalarni yechish\n• Reyting tizimi\n• Sinflar / ligalar bo'yicha bo'linish\n• Yutuqlar uchun mukofotlar\n\n<tg-emoji emoji-id=\"5397782960512444700\">🧐</tg-emoji> <b>Samarqand tuman ixtisoslashtirilgan maktab</b> tomonidan ta'limiy maqsadlarda ishlab chiqilgan."
    await message.answer(about_text, parse_mode='HTML', reply_markup=main_menu_keyboard_no_webapp())

@router.message(F.text == "Yuqori reytinglar")
async def menu_top_ratings(message: Message):
    try:
        import time
        max_retries = 3
        retry_delay = 0.5

        for attempt in range(max_retries):
            try:
                conn = get_db()
                try:
                    top_users = conn.execute("SELECT user_id, username, first_name, last_name, rating, class_name FROM users WHERE rating > 0 ORDER BY rating DESC LIMIT 50").fetchall()
                    top_users = list(top_users)
                finally:
                    conn.close()
                break
            except Exception as db_error:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise db_error

        if not top_users:
            await message.answer("<tg-emoji emoji-id=\"5462927083132970373\">🏆</tg-emoji> Reyting hali bo'sh. Birinchi bo'ling!",parse_mode='HTML', reply_markup=main_menu_keyboard_no_webapp())
            return
        rating_text = "<tg-emoji emoji-id=\"5462927083132970373\">🏆</tg-emoji>  <b>TOP 50 REYTINGLAR</b>\n\n"
        medals = ["<tg-emoji emoji-id=\"5440539497383087970\">🥇</tg-emoji> ", "<tg-emoji emoji-id=\"5447203607294265305\">🥈</tg-emoji> ", "<tg-emoji emoji-id=\"5453902265922376865\">🥉</tg-emoji> "]
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
    if not user:
        return
    photo_url = await get_profile_photo(user.id)
    reply_markup = main_menu_keyboard(user.id, user.first_name, photo_url)
    text = f"<tg-emoji emoji-id=\"5413694143601842851\">👋</tg-emoji> Salom, <b>{user.first_name}</b>! STIM quiz botiga xush kelibsiz!\n\n<tg-emoji emoji-id=\"5406745015365943482\">👇</tg-emoji> <b>Quyidagi tugmalardan foydalaning:</b>"
    await message.answer(text, reply_markup=reply_markup, parse_mode='HTML')

# Callbacks
@router.callback_query(F.data == "check_subscription")
async def callback_check_subscription(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = callback.from_user
    is_subscribed = await verify_subscription(user.id, force_check=True)
    text234 = "<tg-emoji emoji-id=\"5429501538806548545\">✅</tg-emoji> Siz barcha kanallarga obuna bo'ldiz!"
    if is_subscribed:
        await callback.message.answer(text234, parse_mode='HTML')
        await cmd_start(callback.message)
    else:
        await callback.answer("<tg-emoji emoji-id=\"5210952531676504517\">❌</tg-emoji> Siz hamma kanallarga obuna bo'lmagansiz!", show_alert=True)

# Admin callbacks
admin_menu_keyboard = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📢 Xabar jo'natish", callback_data="admin_broadcast")],
    [InlineKeyboardButton(text="📣 Yangi vazifalar haqida e'lon", callback_data="admin_announce")],
    [InlineKeyboardButton(text="📊 Statistika", callback_data="admin_stats")],
    [InlineKeyboardButton(text="👥 Foydalanuvchilar", callback_data="admin_users")],
    [InlineKeyboardButton(text="📝 To'plamlar", callback_data="admin_bundles")],
    [InlineKeyboardButton(text="🔄 Mavsumni qayta boshlash", callback_data="admin_reset_season")],
    [InlineKeyboardButton(text="🔒 Bloklash", callback_data="admin_block_ask")],
    [InlineKeyboardButton(text="🔓 Blokdan chiqarish", callback_data="admin_unblock_ask")],
    [InlineKeyboardButton(text="🔓 Panerni yopish", callback_data="admin_close")]
])

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    # Проверка админа
    if callback.from_user.id != ADMIN_ID:
        await callback.message.edit_text("Sizda ushbu komandaga ruxsat yo'q.")
        return
    await state.set_state(AdminState.waiting_broadcast_text)
    await state.update_data(state='waiting_broadcast_text')
    await callback.message.edit_text("📢 Xabar jo'natish\n\nBarcha foydalanuvchilarga jo'natish uchun matnni kiriting:")

@router.callback_query(F.data == "admin_announce")
async def admin_announce(callback: CallbackQuery):
    await callback.answer()
    text = "<tg-emoji emoji-id=\"5298609030321691620\">📣</tg-emoji> <b>Yangi vazifalar sizni kutmoqda!</b>\n\n<tg-emoji emoji-id=\"5224607267797606837\">⚡️</tg-emoji> Reytingingizni oshirish uchun vazifalarni bajaring!"
    domain = os.getenv('REPLIT_DEV_DOMAIN')
    base_url = f"https://{domain}" if domain else WEBAPP_URL
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=" Vazifalar", style="success", icon_custom_emoji_id="5282843764451195532", web_app=WebAppInfo(url=base_url))]])
    with get_db() as db:
        users = db.execute("SELECT user_id FROM users").fetchall()
    count = 0
    for user in users:
        try:
            await bot.send_message(chat_id=user['user_id'], text=text, parse_mode='HTML', reply_markup=keyboard)
            count += 1
            await asyncio.sleep(0.05)
        except:
            continue
    await callback.message.edit_text(f"✅ Объявление отправлено {count} пользователям.")

@router.callback_query(F.data == "admin_stats")
async def admin_stats_callback(callback: CallbackQuery):
    await callback.answer()
    # Проверка админа
    if callback.from_user.id != ADMIN_ID:
        await callback.message.edit_text("Sizda ushbu komandaga ruxsat yo'q.")
        return
    with get_db() as db:
        total_users = db.execute("SELECT COUNT(*) as cnt FROM users").fetchone()['cnt']
        registered_users = db.execute("SELECT COUNT(*) as cnt FROM users WHERE is_registered = TRUE").fetchone()['cnt']
        total_tasks = db.execute("SELECT COUNT(*) as cnt FROM user_tasks").fetchone()['cnt']
        total_rating = db.execute("SELECT COALESCE(SUM(rating), 0) as total FROM users").fetchone()['total']
        avg_rating = db.execute("SELECT COALESCE(AVG(rating), 0) as avg FROM users WHERE rating > 0").fetchone()['avg']
        bronze = db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('5', '6', '5 класс', '6 класс')").fetchone()['cnt']
        silver = db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('7', '8', '7 класс', '8 класс')").fetchone()['cnt']
        gold = db.execute("SELECT COUNT(*) as cnt FROM users WHERE class_name IN ('9', '10', '11', '9 класс', '10 класс', '11 класс')").fetchone()['cnt']

    stats_text = f"""📊 <b>Bot statistikasi</b>

👥 Jami foydalanuvchilar: {total_users}
✅ Ro'yxatdan o'tganlar: {registered_users}
📝 Jami bajarilgan vazifalar: {total_tasks}
⭐ Umumiy reyting: {total_rating}
📈 O'rtacha reyting: {round(avg_rating, 1) if avg_rating else 0}

🏆 Ligalar:
🥉 Bronza: {bronze}
🥈 Kumush: {silver}
🥇 Oltin: {gold}"""

    await callback.message.edit_text(stats_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")]]))

@router.callback_query(F.data == "admin_users")
async def admin_users_callback(callback: CallbackQuery):
    await callback.answer()
    # Проверка админа
    if callback.from_user.id != ADMIN_ID:
        await callback.message.edit_text("Sizda ushbu komandaga ruxsat yo'q.")
        return
    with get_db() as db:
        users = db.execute("SELECT user_id, username, first_name, last_name, rating, class_name FROM users ORDER BY rating DESC LIMIT 10").fetchall()
    users_text = "👥 <b>TOP 10 foydalanuvchilar</b>\n\n"
    for i, user in enumerate(users, 1):
        name = f"{user['first_name']} {user['last_name']}" if user['first_name'] else (user['username'] or f"User {user['user_id']}")
        users_text += f"{i}. {name} - {user['rating']} ⭐"
        if user['class_name']:
            users_text += f" ({user['class_name']})"
        users_text += "\n"
    users_text += f"\n📊 Jami foydalanuvchilar: {db.execute('SELECT COUNT(*) FROM users').fetchone()[0]}"
    await callback.message.edit_text(users_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📋 Barcha ro'yxat", callback_data="admin_all_users")], [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")]]))

@router.callback_query(F.data == "admin_all_users")
async def admin_all_users_callback(callback: CallbackQuery):
    await callback.answer()
    users_text = "👥 <b>Barcha foydalanuvchilar</b>\n\n"
    with get_db() as db:
        users = db.execute("SELECT user_id, username, first_name, last_name, rating, class_name FROM users ORDER BY rating DESC LIMIT 20").fetchall()
        for user in users:
            name = f"{user['first_name']} {user['last_name']}" if user['first_name'] else (user['username'] or f"User {user['user_id']}")
            users_text += f"• {name} - {user['rating']} ⭐"
            if user['class_name']:
                users_text += f" ({user['class_name']})"
            users_text += "\n"
    await callback.message.edit_text(users_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")]]))

@router.callback_query(F.data == "admin_bundles")
async def admin_bundles_callback(callback: CallbackQuery):
    await callback.answer()
    # Проверка админа
    if callback.from_user.id != ADMIN_ID:
        await callback.message.edit_text("Sizda ushbu komandaga ruxsat yo'q.")
        return
    with get_db() as db:
        bundles = db.execute("SELECT * FROM task_bundles").fetchall()
    bundles_text = "📝 <b>Vazifalar to'plamlari</b>\n\n"
    for b in bundles:
        questions_count = db.execute("SELECT COUNT(*) FROM bundle_questions WHERE bundle_id = ?", (b['id'],)).fetchone()[0]
        bundles_text += f"• {b['name']} - {questions_count} savol\n"
    bundles_text += f"\n📊 Jami to'plamlar: {len(bundles)}"
    await callback.message.edit_text(bundles_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ To'plam qo'shish", callback_data="admin_add_bundle")], [InlineKeyboardButton(text="📋 Barcha to'plamlar", callback_data="admin_view_bundles")], [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")]]))

@router.callback_query(F.data == "admin_add_bundle")
async def admin_add_bundle_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminState.waiting_bundle_name)
    await state.update_data(state='waiting_bundle_name', bundle_questions=[])
    await callback.message.edit_text("📝 <b>Yangi to'plam yaratish</b>\n\nYangi to'plam uchun nom kiriting:\n(masalan: \"Matematika - 5-sinf\" yoki \"Yakuniy test\")", parse_mode='HTML')

@router.callback_query(F.data == "admin_view_bundles")
async def admin_view_bundles_callback(callback: CallbackQuery):
    await callback.answer()
    with get_db() as db:
        bundles = db.execute("SELECT * FROM task_bundles ORDER BY created_at DESC").fetchall()
    if not bundles:
        bundles_text = "📋 <b>Barcha to'plamlar</b>\n\nTo'plamlar hozircha yo'q."
    else:
        bundles_text = "📋 <b>Barcha to'plamlar</b>\n\n"
        for b in bundles:
            questions_count = db.execute("SELECT COUNT(*) FROM bundle_questions WHERE bundle_id = ?", (b['id'],)).fetchone()[0]
            created_date = b['created_at'][:10] if b['created_at'] else 'nomalum'
            bundles_text += f"📦 {b['name']}\n   └ {questions_count} savol | Yaratilgan: {created_date}\n\n"
    await callback.message.edit_text(bundles_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_bundles")]]))

@router.callback_query(F.data == "admin_reset_season")
async def admin_reset_season_callback(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("⚠️ <b>E'tibor!</b>\n\nMavsumni qayta boshlashni istaysizmi?\n\nBu amal quyidagilarni o'chiradi:\n• Barcha foydalanuvchilarning reytingi\n• Barcha bajarilgan vazifalar tarixi\n\nBu amaldan keyin tiklab bo'lmaydi!", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[InlineKeyboardButton(text="✅ Ha, qayta boshlash", callback_data="admin_reset_confirm"), InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin_back")]))

@router.callback_query(F.data == "admin_reset_confirm")
async def admin_reset_confirm_callback(callback: CallbackQuery):
    await callback.answer()
    with get_db() as db:
        db.execute("UPDATE users SET rating = 0")
        db.execute("DELETE FROM user_tasks")
        db.execute("UPDATE system_settings SET value = ? WHERE key = 'season_start'", (datetime.now().isoformat(),))
        db.commit()
        github_backup.auto_push()
    await callback.message.edit_text("✅ Mavsum muvaffaqiyatli qayta boshlash!\n\nBarcha reytinglar va vazifalar tarixi tozalandi.")

@router.callback_query(F.data == "admin_close")
async def admin_close_callback(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("🔒 Admin paneli yopildi.")

@router.callback_query(F.data == "admin_block_user")
async def admin_block_user_callback(callback: CallbackQuery):
    await callback.answer()
    with get_db() as db:
        users = db.execute("SELECT user_id, username, first_name, last_name, is_blocked FROM users ORDER BY rating DESC LIMIT 15").fetchall()
    users_text = "🔒 <b>Foydalanuvchilarni bloklash</b>\n\nBloklash/ochish uchun foydalanuvchini tanlang:\n\n"
    keyboard = []
    for user in users:
        name = f"{user['first_name']} {user['last_name']}" if user['first_name'] else (user['username'] or f"User {user['user_id']}")
        status = "🔴" if user['is_blocked'] else "🟢"
        users_text += f"{status} {name}\n"
        callback_data = f"admin_unblock_{user['user_id']}" if user['is_blocked'] else f"admin_block_{user['user_id']}"
        btn_text = "🔓 Blokdan chiqarish" if user['is_blocked'] else "🔒 Bloklash"
        keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    await callback.message.edit_text(users_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

@router.callback_query(F.data.startswith("admin_block_"))
async def admin_block_specific_callback(callback: CallbackQuery):
    await callback.answer()
    user_id = int(callback.data.split("_")[-1])
    with get_db() as db:
        db.execute("UPDATE users SET is_blocked = TRUE WHERE user_id = ?", (user_id,))
        db.commit()
        github_backup.auto_push()
    await callback.message.edit_text(f"✅ Пользователь {user_id} заблокирован.")

@router.callback_query(F.data.startswith("admin_unblock_"))
async def admin_unblock_specific_callback(callback: CallbackQuery):
    await callback.answer()
    user_id = int(callback.data.split("_")[-1])
    with get_db() as db:
        db.execute("UPDATE users SET is_blocked = FALSE WHERE user_id = ?", (user_id,))
        db.commit()
        github_backup.auto_push()
    await callback.message.edit_text(f"✅ Пользователь {user_id} разблокирован.")

@router.callback_query(F.data == "admin_back")
async def admin_back_callback(callback: CallbackQuery):
    await callback.answer()
    # Проверка админа
    if callback.from_user.id != ADMIN_ID:
        await callback.message.edit_text("Sizda ushbu komandaga ruxsat yo'q.")
        return
    await callback.message.edit_text("Admin panelga xush kelibsiz:", reply_markup=admin_menu_keyboard)

@router.callback_query(F.data == "admin_block_ask")
async def admin_block_ask_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    # Проверка админа
    if callback.from_user.id != ADMIN_ID:
        await callback.message.edit_text("Sizda ushbu komandaga ruxsat yo'q.")
        return
    await state.set_state(AdminState.waiting_block_user_id)
    await state.update_data(state='waiting_block_user_id')
    await callback.message.edit_text("🔒 <b>Блокировка пользователя</b>\n\nВведите Telegram ID пользователя, которого хотите заблокировать:", parse_mode='HTML')

@router.callback_query(F.data == "admin_unblock_ask")
async def admin_unblock_ask_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    # Проверка админа
    if callback.from_user.id != ADMIN_ID:
        await callback.message.edit_text("Sizda ushbu komandaga ruxsat yo'q.")
        return
    await state.set_state(AdminState.waiting_unblock_user_id)
    await state.update_data(state='waiting_unblock_user_id')
    await callback.message.edit_text("🔓 <b>Разблокировка пользователя</b>\n\nВведите Telegram ID пользователя, которого хотите разблокировать:", parse_mode='HTML')

# Admin message handlers
@router.message(AdminState.waiting_broadcast_text)
async def admin_broadcast_text(message: Message, state: FSMContext):
    broadcast_text = message.text
    await state.clear()
    with get_db() as db:
        users = db.execute("SELECT user_id FROM users").fetchall()
    count = 0
    for user in users:
        try:
            await bot.send_message(chat_id=user['user_id'], text=broadcast_text)
            count += 1
            await asyncio.sleep(0.05)
        except:
            continue
    await message.answer(f"📢 Xabar jo'natildi {count} foydalanuvchiga.")

@router.message(AdminState.waiting_bundle_name)
async def admin_bundle_name(message: Message, state: FSMContext):
    bundle_name = message.text.strip()
    if len(bundle_name) < 3:
        await message.answer("❌ Название слишком короткое. Введите название сборки (минимум 3 символа):")
        return
    await state.update_data(bundle_name=bundle_name, bundle_questions=[])
    await state.set_state(AdminState.waiting_bundle_question)
    await message.answer(f"📝 <b>Сборка: {bundle_name}</b>\n\nТеперь добавьте вопросы в формате:\n<code>Вопрос?|Вариант1|Вариант2|Вариант3|0|5</code>\n\nГде:\n• Вопрос - текст вопроса\n• Варианты - ответы через | (минимум 2)\n• 0 - индекс правильного ответа\n• 5 - баллы за правильный ответ\n\n<b>Пример:</b>\nСтолица Франции?|Берлин|Lондон|пАРИЖ|2|10\n\n<b>Для завершения введите:</b> /done", parse_mode='HTML')

@router.message(AdminState.waiting_bundle_question)
async def admin_bundle_question(message: Message, state: FSMContext):
    question_text = message.text.strip()
    if question_text.lower() == '/done':
        data = await state.get_data()
        bundle_name = data.get('bundle_name')
        questions = data.get('bundle_questions', [])
        if not questions:
            await message.answer("❌ Siz birorta savol qo'shmadingiz! To'plam bekor qilindi.")
            await state.clear()
            return
        try:
            with get_db() as db:
                cursor = db.execute("INSERT INTO task_bundles (name) VALUES (?)", (bundle_name,))
                bundle_id = cursor.lastrowid
                for q in questions:
                    db.execute("INSERT INTO bundle_questions (bundle_id, question, options, correct_option, rating) VALUES (?, ?, ?, ?, ?)", (bundle_id, q['question'], q['options'], q['correct_option'], q['rating']))
                db.commit()
                github_backup.auto_push()
            await message.answer(f"✅ <b>Сборка создана!</b>\n\n📦 Название: {bundle_name}\n📝 Вопросов: {len(questions)}\n\nСборка теперь доступна в Mini App.", parse_mode='HTML')
        except Exception as e:
            await message.answer(f"❌ To'plamni yaratishda xatolik yuz berdi: {str(e)}")
        await state.clear()
        return

    try:
        parts = question_text.split('|')
        if len(parts) < 3:
            await message.answer("❌ Неверный формат! Используйте:\n<code>Вопрос?|Вариант1|Вариант2|0|5</code>\n\nМинимум: вопрос и 2 варианта через |", parse_mode='HTML')
            return
        question = parts[0].strip()
        options = '|'.join(parts[1:-2]) if len(parts) > 3 else parts[1].strip()
        correct_option = int(parts[-2].strip()) if len(parts) >= 3 else 0
        rating = int(parts[-1].strip()) if len(parts) > 3 else 5
        if not question or not options:
            await message.answer("❌ Вопрос и варианты не могут быть пустыми!")
            return
        data = await state.get_data()
        questions = data.get('bundle_questions', [])
        questions.append({'question': question, 'options': options, 'correct_option': correct_option, 'rating': rating})
        await state.update_data(bundle_questions=questions)
        await message.answer(f"✅ Вопрос {len(questions)} добавлен!\n\nДобавьте еще вопросы или введите /done для завершения.", parse_mode='HTML')
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке вопроса: {str(e)}")

@router.message(AdminState.waiting_block_user_id)
async def admin_block_user_id(message: Message, state: FSMContext):
    user_id_to_block = message.text.strip()
    await state.clear()
    try:
        target_user_id = int(user_id_to_block)
    except ValueError:
        await message.answer("❌ Неверный формат ID. Введите числовой Telegram ID.")
        return
    with get_db() as db:
        user = db.execute("SELECT * FROM users WHERE user_id = ?", (target_user_id,)).fetchone()
        if not user:
            await message.answer(f"❌ Пользователь с ID {target_user_id} не найден в базе данных.")
            return
        if user['is_blocked']:
            await message.answer(f"ℹ️ Пользователь с ID {target_user_id} уже заблокирован.")
            return
        db.execute("UPDATE users SET is_blocked = TRUE WHERE user_id = ?", (target_user_id,))
        db.commit()
        github_backup.auto_push()
    await message.answer(f"✅ Пользователь с ID {target_user_id} заблокирован.\n\nОн не сможет использовать бота и Mini App.")

@router.message(AdminState.waiting_unblock_user_id)
async def admin_unblock_user_id(message: Message, state: FSMContext):
    user_id_to_unblock = message.text.strip()
    await state.clear()
    try:
        target_user_id = int(user_id_to_unblock)
    except ValueError:
        await message.answer("❌ Неверный формат ID. Введите числовой Telegram ID.")
        return
    with get_db() as db:
        user = db.execute("SELECT * FROM users WHERE user_id = ?",     (target_user_id,)).fetchone()
        if not user:
            await message.answer(f"❌ Пользователь с ID {target_user_id} не найден в базе данных.")
            return
        if not user['is_blocked']:
            await message.answer(f"ℹ️ Пользователь с ID {target_user_id} не заблокирован.")
            return
        db.execute("UPDATE users SET is_blocked = FALSE WHERE user_id = ?", (target_user_id,))
        db.commit()
        github_backup.auto_push()
    await message.answer(f"✅ Пользователь с ID {target_user_id} разблокирован.\n\nОн снова может использовать бота и Mini App.")

dp.include_router(router)


# Function to clean up old rate limiting data
def cleanup_rate_limit_data():
    """Background task to clean up old rate limiting data."""
    while True:
        try:
            current_time = time.time()

            # Clean old user timestamps
            for user_id in list(user_message_timestamps.keys()):
                user_message_timestamps[user_id] = [
                    ts for ts in user_message_timestamps[user_id]
                    if current_time - ts < SPAM_TIME_WINDOW
                ]
                if not user_message_timestamps[user_id]:
                    del user_message_timestamps[user_id]

            # Clean old IP timestamps
            for ip in list(ip_request_timestamps.keys()):
                ip_request_timestamps[ip] = [
                    ts for ts in ip_request_timestamps[ip]
                    if current_time - ts < FLASK_RATE_WINDOW
                ]
                if not ip_request_timestamps[ip]:
                    del ip_request_timestamps[ip]

            logger.debug("Rate limit data cleaned up")

        except Exception as e:
            logger.error(f"Error cleaning up rate limit data: {e}")

        time.sleep(60)  # Run every minute


async def main():
    # Configure GitHub backup first
    github_token = os.environ.get('GITHUB_TOKEN', '')
    github_repo = os.environ.get('GITHUB_REPO', '')
    if github_token and github_repo:
        github_backup.configure(
            github_token=github_token,
            github_repo=github_repo,
            github_branch=os.environ.get('GITHUB_BRANCH', 'main'),
            db_path='bot.db',
            backup_path=os.environ.get('BACKUP_PATH', 'backups'),
            auto_save_interval=int(os.environ.get('AUTO_SAVE_INTERVAL', 300))
        )
        # Try to restore from GitHub BEFORE init_db
        logger.info("Checking for existing backup in GitHub...")
        if github_backup.restore_from_github():
            logger.info("Database restored from GitHub successfully")
        else:
            logger.info("No backup found in GitHub, starting with fresh database")
    else:
        logger.info("GitHub auto-backup not configured (set GITHUB_TOKEN and GITHUB_REPO env vars)")
    
    init_db()

    # Start cleanup thread
    cleanup_thread = threading.Thread(target=cleanup_rate_limit_data, daemon=True)
    cleanup_thread.start()

    # Start GitHub auto-backup
    if github_token and github_repo:
        github_backup.start_auto_save()
        logger.info("GitHub auto-backup started")

    threading.Thread(target=run_flask, daemon=True).start()
    logger.info("Бот запущен (aiogram 3) с анти-спам и анти-DDoS защитой")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, allowed_updates=['message', 'callback_query'])
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
