import os
import re
import json
import time
import threading
import subprocess
import requests as req
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

def now_vn():
    try:
        from datetime import timezone
        return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=7))).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow() + timedelta(hours=7)

from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# ============================================================
# CONFIGURATION & ENVIRONMENT
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')

DEFAULT_CONFIG = {
    'telegram_bot_token': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'telegram_chat_id': os.environ.get('TELEGRAM_CHAT_ID', ''),
    'cf_worker_url': os.environ.get('CF_WORKER_URL', ''), # Cloudflare Worker Edge Scanner
    'check_interval': 60,
    'use_proxy_pool': True,
    'allowed_users': {},
    'users': {}
}

# ============================================================
# GLOBAL STATE
# ============================================================
monitor_thread = None
is_monitoring = True
logs = []
uid_alive_status = {}  # {uid: True/False/None}
last_checked_time = {} # {uid: datetime}
http_session = req.Session()
config_lock = threading.RLock()

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
}

# ============================================================
# DATABASE (MongoDB Atlas Cloud)
# ============================================================
MONGO_URI = os.environ.get('MONGO_URI', '').strip()
mongo_client = None
mongo_db = None

def get_mongo_db():
    global mongo_client, mongo_db
    if mongo_db is not None:
        return mongo_db
    if not MONGO_URI:
        return None
    try:
        import pymongo
        mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')
        mongo_db = mongo_client['fb_live_monitor']
        return mongo_db
    except Exception as e:
        try:
            add_log(f"MongoDB connection error: {e}", 'error')
        except Exception:
            print(f"MongoDB connection error: {e}")
        return None

def load_config():
    with config_lock:
        db = get_mongo_db()
        if db is not None:
            try:
                doc = db['config'].find_one({'_id': 'app_config'})
                if doc:
                    doc.pop('_id', None)
                    return doc
            except Exception as e:
                add_log(f"MongoDB load_config error: {e}", 'error')

        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
            except Exception:
                cfg = DEFAULT_CONFIG.copy()
        else:
            cfg = DEFAULT_CONFIG.copy()
            
        if 'uids' in cfg and 'users' not in cfg:
            admin_id = str(cfg.get('telegram_chat_id', ''))
            cfg['users'] = {}
            if admin_id and cfg['uids']:
                cfg['users'][admin_id] = cfg.pop('uids')
            else:
                cfg.pop('uids', None)
            save_config(cfg)
        return cfg

def save_config(config):
    with config_lock:
        db = get_mongo_db()
        if db is not None:
            try:
                cfg_to_save = config.copy()
                cfg_to_save['_id'] = 'app_config'
                db['config'].replace_one({'_id': 'app_config'}, cfg_to_save, upsert=True)
            except Exception as e:
                add_log(f"MongoDB save_config error: {e}", 'error')

        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            add_log(f"File save_config error: {e}", 'error')

def add_log(message, level='info'):
    ts = now_vn().strftime('%H:%M:%S %d/%m/%Y')
    logs.append({'timestamp': ts, 'message': message, 'level': level})
    if len(logs) > 500:
        logs.pop(0)
    try:
        print(f"[{ts}] [{level.upper()}] {message}")
    except Exception:
        pass

# ============================================================
# FREE ROTATING PROXY POOL (Ý TƯỞNG 4: Auto Scrape & Rotate)
# ============================================================
class FreeProxyPool:
    def __init__(self):
        self.proxies = []
        self.last_updated = None
        self.lock = threading.Lock()

    def refresh_proxies(self):
        sources = [
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt',
            'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt',
            'https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt'
        ]
        scraped = set()
        for src in sources:
            try:
                r = req.get(src, timeout=8)
                if r.status_code == 200:
                    for line in r.text.splitlines():
                        line = line.strip()
                        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5}$', line):
                            scraped.add(line)
            except Exception:
                continue

        with self.lock:
            if scraped:
                self.proxies = list(scraped)
                self.last_updated = now_vn()
                add_log(f"🛡️ Đã nạp {len(self.proxies)} Free Proxy từ GitHub để xoay vòng", 'info')

    def get_random_proxy(self):
        with self.lock:
            if not self.proxies or (self.last_updated and (now_vn() - self.last_updated).total_seconds() > 3600):
                threading.Thread(target=self.refresh_proxies, daemon=True).start()
            if self.proxies:
                p = random.choice(self.proxies)
                return {'http': f'http://{p}', 'https': f'http://{p}'}
            return None

proxy_pool = FreeProxyPool()
threading.Thread(target=proxy_pool.refresh_proxies, daemon=True).start()

# ============================================================
# FACEBOOK CHECK ENGINE (CLOUDFLARE WORKER & HYBRID SCANNER)
# ============================================================
def check_uids_via_cf_worker(uids_list, worker_url):
    """
    Ý TƯỞNG 3: Gửi danh sách UID lên Cloudflare Worker Edge để quét song song.
    Tận dụng hàng triệu IP Anycast toàn cầu của Cloudflare -> 0 rate limit.
    """
    if not worker_url or not uids_list:
        return None

    clean_url = worker_url.rstrip('/')
    if not clean_url.endswith('/check-batch'):
        endpoint = f"{clean_url}/check-batch"
    else:
        endpoint = clean_url

    try:
        payload = {'uids': uids_list}
        r = req.post(endpoint, json=payload, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if data.get('success') and 'results' in data:
                # Map kết quả {uid: {'status': 'alive'|'dead', 'avatar': '...', 'reason': '...'}}
                res_map = {}
                for item in data['results']:
                    res_map[item['uid']] = {
                        'status': item.get('status', 'error'),
                        'avatar': item.get('avatar', ''),
                        'reason': item.get('reason', '')
                    }
                add_log(f"⚡ [Cloudflare Edge] Đã quét xong {len(res_map)} UID siêu tốc!", 'success')
                return res_map
    except Exception as e:
        add_log(f"⚠️ Cloudflare Worker error: {e}, chuyển sang quét fallback", 'warning')
    return None

def check_uid_alive(uid):
    """
    Kiểm tra 1 UID:
    Ưu tiên 1: Cloudflare Worker nếu có URL
    Ưu tiên 2: Direct Graph API (scontent vs static)
    Ưu tiên 3: Free Proxy Pool nếu direct bị lỗi
    Ưu tiên 4: mbasic (nếu có cookie)
    """
    config = load_config()
    cf_url = config.get('cf_worker_url', '')

    # Thử qua Cloudflare Worker nếu có
    if cf_url:
        cf_res = check_uids_via_cf_worker([uid], cf_url)
        if cf_res and uid in cf_res:
            st = cf_res[uid]['status']
            if st in ('alive', 'dead'):
                return st

    # Phương pháp Graph API Picture
    try:
        url = f"https://graph.facebook.com/v19.0/{uid}/picture?redirect=false"
        r = http_session.get(url, timeout=12)
        if r.status_code == 200:
            data = r.json().get('data', {})
            pic_url = data.get('url', '')
            has_dim = 'height' in data and 'width' in data

            if 'static.xx.fbcdn.net' in pic_url and not has_dim:
                return 'dead'
            elif 'scontent' in pic_url and has_dim:
                return 'alive'
            elif has_dim:
                return 'alive'
            else:
                return 'dead'
        elif r.status_code in (400, 404):
            return 'dead'
    except Exception:
        pass

    # Thử qua Proxy Pool nếu gặp lỗi
    proxy = proxy_pool.get_random_proxy()
    if proxy:
        try:
            url = f"https://graph.facebook.com/v19.0/{uid}/picture?redirect=false"
            r = req.get(url, proxies=proxy, timeout=10)
            if r.status_code == 200:
                data = r.json().get('data', {})
                pic_url = data.get('url', '')
                if 'static.xx.fbcdn.net' in pic_url:
                    return 'dead'
                return 'alive'
            elif r.status_code in (400, 404):
                return 'dead'
        except Exception:
            pass

    return 'dead' # Mặc định an toàn

# ============================================================
# TELEGRAM NOTIFICATIONS
# ============================================================
def send_telegram(bot_token, chat_id, message, reply_markup=None):
    if not bot_token or not chat_id:
        return False, 'Thiếu bot token hoặc chat ID'
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'HTML'}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        r = req.post(url, json=payload, timeout=15)
        result = r.json()
        if result.get('ok'):
            return True, ''
        else:
            detail = f"[{result.get('error_code','?')}] {result.get('description','?')}"
            return False, detail
    except Exception as e:
        return False, str(e)

def send_telegram_photo(bot_token, chat_id, photo_url, caption, reply_markup=None):
    if not bot_token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
        payload = {'chat_id': chat_id, 'photo': photo_url, 'caption': caption, 'parse_mode': 'HTML'}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        r = req.post(url, json=payload, timeout=15)
        if not r.json().get('ok'):
            return send_telegram(bot_token, chat_id, caption, reply_markup)[0]
        return True
    except Exception:
        return send_telegram(bot_token, chat_id, caption, reply_markup)[0]

# ============================================================
# USER & ACCESS EXTENSION HELPERS
# ============================================================
def extend_access(allowed_users, chat_id, days):
    current = allowed_users.get(str(chat_id))
    if current == 'permanent':
        return 'permanent', False
    base = now_vn()
    if current:
        try:
            current_dt = datetime.strptime(current, '%Y-%m-%d %H:%M:%S')
            if current_dt > base:
                base = current_dt
        except Exception:
            pass
    new_expiry = (base + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    allowed_users[str(chat_id)] = new_expiry
    return new_expiry, True

def get_bot_username(bot_token):
    try:
        data = req.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=10).json()
        return data.get('result', {}).get('username', '') if data.get('ok') else ''
    except Exception:
        return ''

def create_pending_referral(referrer_id, referred_id):
    referrer_id, referred_id = str(referrer_id), str(referred_id)
    if not referrer_id or referrer_id == referred_id:
        return False
    db = get_mongo_db()
    if db is None:
        return False
    try:
        if db['referrals'].find_one({'referred_id': referred_id}):
            return False
        db['referrals'].insert_one({
            '_id': f"ref_{referred_id}", 'referrer_id': referrer_id,
            'referred_id': referred_id, 'status': 'pending', 'reward_days': 15,
            'created_at': now_vn().strftime('%Y-%m-%d %H:%M:%S'),
            'qualified_at': None, 'rewarded_at': None
        })
        return True
    except Exception as e:
        add_log(f"Referral create error: {e}", 'error')
        return False

def reward_pending_referral(referred_id, config, bot_token, admin_chat_id):
    db = get_mongo_db()
    if db is None:
        return False
    try:
        from pymongo import ReturnDocument
        referral = db['referrals'].find_one_and_update(
            {'referred_id': str(referred_id), 'status': 'pending'},
            {'$set': {'status': 'processing', 'qualified_at': now_vn().strftime('%Y-%m-%d %H:%M:%S')}},
            return_document=ReturnDocument.AFTER
        )
        if not referral:
            return False
        referrer_id = referral['referrer_id']
        allowed_users = config.setdefault('allowed_users', {})
        if allowed_users.get(referrer_id) == 'permanent':
            db['referral_bonus'].update_one({'_id': referrer_id}, {'$inc': {'days': 15}}, upsert=True)
            expiry_text = 'Gói vĩnh viễn — 15 ngày đã lưu vào quỹ thưởng'
        elif referrer_id in allowed_users:
            expiry_text, _ = extend_access(allowed_users, referrer_id, 15)
        else:
            expiry_text, _ = extend_access(allowed_users, referrer_id, 15)
            config.setdefault('users', {}).setdefault(referrer_id, [])
        save_config(config)
        rewarded_at = now_vn().strftime('%Y-%m-%d %H:%M:%S')
        db['referrals'].update_one({'_id': referral['_id'], 'status': 'processing'}, {'$set': {'status': 'rewarded', 'rewarded_at': rewarded_at, 'new_expiry': expiry_text}})
        send_telegram(bot_token, referrer_id, f"🎉 <b>GIỚI THIỆU THÀNH CÔNG!</b>\nBạn được cộng <b>15 ngày</b>.\n⏰ Hạn mới: <b>{expiry_text}</b>")
        send_telegram(bot_token, referred_id, "✅ Bạn đã hoàn tất điều kiện. Người giới thiệu đã nhận 15 ngày thưởng!")
        if admin_chat_id:
            send_telegram(bot_token, admin_chat_id, f"👑 <b>REFERRAL THÀNH CÔNG</b>\nNgười mời: <code>{referrer_id}</code> (+15 ngày)\nNgười mới: <code>{referred_id}</code>")
        return True
    except Exception as e:
        add_log(f"Referral reward error: {e}", 'error')
        return False

def referral_stats(chat_id):
    db = get_mongo_db()
    if db is None:
        return 0, 0, 0
    try:
        total = db['referrals'].count_documents({'referrer_id': str(chat_id)})
        rewarded = db['referrals'].count_documents({'referrer_id': str(chat_id), 'status': 'rewarded'})
        pending = db['referrals'].count_documents({'referrer_id': str(chat_id), 'status': 'pending'})
        return total, rewarded, pending
    except Exception:
        return 0, 0, 0

# ============================================================
# VIDEO DOWNLOADERS (TikTok & Facebook)
# ============================================================
def download_tiktok_video(url):
    try:
        api_url = f"https://www.tikwm.com/api/?url={url}"
        r = req.get(api_url, timeout=20)
        data = r.json()
        if data.get('code') == 0 and data.get('data'):
            video_url = data['data'].get('play')
            if video_url:
                if not video_url.startswith('http'):
                    video_url = 'https://www.tikwm.com' + video_url
                return video_url, None
        return None, 'Không lấy được video từ TikTok.'
    except Exception as e:
        return None, str(e)

def download_fb_video(url):
    try:
        tmp_file = os.path.join(BASE_DIR, 'tmp_fb_video.mp4')
        if os.path.exists(tmp_file):
            try: os.remove(tmp_file)
            except: pass
        ytdlp_path = 'yt-dlp'
        cmd = [ytdlp_path, '-f', 'best[ext=mp4]/best', '--no-playlist', '-o', tmp_file, url]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if os.path.exists(tmp_file) and os.path.getsize(tmp_file) > 0:
            file_size = os.path.getsize(tmp_file)
            return tmp_file, file_size, None
        else:
            return None, 0, result.stderr or 'Không tải được video.'
    except Exception as e:
        return None, 0, str(e)

def is_tiktok_url(text):
    return bool(re.search(r'(tiktok\.com|vm\.tiktok\.com)', text, re.IGNORECASE))

def is_facebook_video_url(text):
    return bool(re.search(r'(facebook\.com|fb\.watch|fb\.com).*(video|reel|watch|share)', text, re.IGNORECASE))

# ============================================================
# MONITORING LOOP (DUAL MODE: CANH DIE & CANH KHÁNG VỀ)
# ============================================================
def monitor_loop():
    global is_monitoring, last_checked_time, uid_alive_status

    add_log("🚀 Bắt đầu vòng lặp theo dõi Facebook UID (Dual Mode: Canh DIE & Canh Kháng Về)...", 'success')

    while is_monitoring:
        config = load_config()
        bot_token = config.get('telegram_bot_token', '')
        cf_url = config.get('cf_worker_url', '')
        interval = max(10, config.get('check_interval', 60))

        if not bot_token:
            time.sleep(interval)
            continue

        users = config.get('users', {})
        if not users:
            time.sleep(interval)
            continue

        # Gom nhóm UID
        uid_map = {}
        for chat_id, uid_list in users.items():
            for u in uid_list:
                uid = u.get('uid', '')
                if not uid: continue
                name = u.get('name', uid)
                added_at = u.get('added_at', '')
                monitor_type = u.get('monitor_type', 'WATCH_REVIVE') # 'WATCH_DIE' hoặc 'WATCH_REVIVE'
                
                if uid not in uid_map:
                    uid_map[uid] = []
                uid_map[uid].append({
                    'chat_id': chat_id,
                    'name': name,
                    'added_at': added_at,
                    'monitor_type': monitor_type
                })

        all_uids = list(uid_map.keys())
        batch_results = {}

        # Nếu có Cloudflare Worker -> Quét hàng loạt cực nhanh
        if cf_url and all_uids:
            batch_results = check_uids_via_cf_worker(all_uids, cf_url) or {}

        # Xử lý kết quả từng UID
        for uid, followers in uid_map.items():
            if not is_monitoring:
                break

            if uid in batch_results:
                status = batch_results[uid]['status']
            else:
                status = check_uid_alive(uid)

            last_checked_time[uid] = now_vn()
            was_alive = uid_alive_status.get(uid)

            # -------------------------------------------------------------
            # TRƯỜNG HỢP 1: NICK SỐNG LẠI (ALIVE)
            # -------------------------------------------------------------
            if status == 'alive':
                # Nếu trước đó là Dead hoặc chưa biết, và đang có người canh Kháng về
                if was_alive is False or was_alive is None:
                    uid_alive_status[uid] = True
                    for f in followers:
                        cid = f['chat_id']
                        name = f['name']
                        added_at = f.get('added_at', '')
                        m_type = f.get('monitor_type', 'WATCH_REVIVE')

                        # Chỉ bắn thông báo Sống lại cho người đặt chế độ Canh Kháng (hoặc lần đầu)
                        if m_type == 'WATCH_REVIVE' and was_alive is False:
                            processing_time_str = "Không rõ"
                            if added_at:
                                try:
                                    t1 = datetime.strptime(added_at, '%Y-%m-%d %H:%M:%S')
                                    diff = int((now_vn() - t1).total_seconds())
                                    hours = diff // 3600
                                    minutes = (diff % 3600) // 60
                                    seconds = diff % 60
                                    if hours > 0: processing_time_str = f"{hours}h {minutes}m {seconds}s"
                                    elif minutes > 0: processing_time_str = f"{minutes}m {seconds}s"
                                    else: processing_time_str = f"{seconds}s"
                                except Exception: pass

                            msg = (
                                f"🎉 <b>[TIN VUI] NICK FACEBOOK ĐÃ SỐNG LẠI!</b> 🟢✨\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"🆔 <b>UID:</b> <code>{uid}</code>\n"
                                f"👤 <b>Tên/Ghi chú:</b> {name}\n"
                                f"⏰ <b>Thời gian mở khóa:</b> {now_vn().strftime('%H:%M:%S | %d/%m/%Y')}\n"
                                f"⏳ <b>Thời gian xử lý:</b> {processing_time_str}\n"
                                f"✅ <b>Trạng thái:</b> Đã hoạt động trở lại (Kháng về thành công!)\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"👉 <i>Vui lòng vào kiểm tra, bàn giao hoặc đổi bảo mật!</i>"
                            )
                            rm = {
                                "inline_keyboard": [
                                    [{"text": "✅ Done kèo (Xóa)", "callback_data": f"del_{uid}"}, {"text": "🌐 Xem Facebook", "url": f"https://facebook.com/{uid}"}],
                                    [{"text": "🔄 Chuyển sang Canh DIE", "callback_data": f"swdie_{uid}"}]
                                ]
                            }
                            photo_url = f"https://graph.facebook.com/{uid}/picture?type=large&redirect=true"
                            send_telegram_photo(bot_token, cid, photo_url, msg, rm)
                            add_log(f"🟢 [KHÁNG VỀ] {uid} sống lại! Gửi tin cho {cid}", 'success')

                            # Báo Admin
                            admin_id = str(config.get('telegram_chat_id', ''))
                            if admin_id and cid != admin_id:
                                admin_msg = f"👑 <b>[BÁO CÁO ADMIN] UID VỪA SỐNG LẠI!</b>\n🆔 <code>{uid}</code> | Khách: <code>{cid}</code>"
                                send_telegram_photo(bot_token, admin_id, photo_url, admin_msg)

            # -------------------------------------------------------------
            # TRƯỜNG HỢP 2: NICK BỊ DIE (DEAD)
            # -------------------------------------------------------------
            elif status == 'dead':
                # Nếu trước đó đang Live ➔ Bị DIE
                if was_alive is True:
                    uid_alive_status[uid] = False
                    for f in followers:
                        cid = f['chat_id']
                        name = f['name']
                        m_type = f.get('monitor_type', 'WATCH_DIE')

                        # Bắn cảnh báo DIE cho người canh DIE
                        if m_type == 'WATCH_DIE':
                            msg = (
                                f"🚨 <b>[CẢNH BÁO] NICK FACEBOOK BỊ DIE!</b> 🔴\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"🆔 <b>UID:</b> <code>{uid}</code>\n"
                                f"👤 <b>Tên/Ghi chú:</b> {name}\n"
                                f"⏰ <b>Thời gian phát hiện:</b> {now_vn().strftime('%H:%M:%S | %d/%m/%Y')}\n"
                                f"⚠️ <b>Trạng thái:</b> 🔴 <b>DIE / Checkpoint 282/956 / Bị khóa</b>\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"👉 <i>Hãy kiểm tra chiến dịch Ads, via cầm hoặc thay nick mới ngay!</i>"
                            )
                            rm = {
                                "inline_keyboard": [
                                    [{"text": "🗑️ Xóa UID", "callback_data": f"del_{uid}"}, {"text": "🌐 Xem Facebook", "url": f"https://facebook.com/{uid}"}],
                                    [{"text": "🔄 Chuyển sang Canh Kháng (Sống lại)", "callback_data": f"swrevive_{uid}"}]
                                ]
                            }
                            photo_url = f"https://graph.facebook.com/{uid}/picture?type=large&redirect=true"
                            send_telegram_photo(bot_token, cid, photo_url, msg, rm)
                            add_log(f"🔴 [NICK DIE] {uid} bị DIE! Báo động tới {cid}", 'error')

                            # Báo Admin
                            admin_id = str(config.get('telegram_chat_id', ''))
                            if admin_id and cid != admin_id:
                                admin_msg = f"👑 <b>[BÁO CÁO ADMIN] UID BỊ DIE!</b>\n🆔 <code>{uid}</code> | Khách: <code>{cid}</code>"
                                send_telegram_photo(bot_token, admin_id, photo_url, admin_msg)

                elif was_alive is None:
                    uid_alive_status[uid] = False

        time.sleep(interval)

# ============================================================
# TELEGRAM BOT INTERACTION (POLLING & COMMANDS)
# ============================================================
telegram_offset = 0

def telegram_bot_polling():
    global telegram_offset, is_monitoring, monitor_thread

    time.sleep(2)
    add_log("🤖 Bắt đầu lắng nghe Telegram Bot (Hỗ trợ Canh DIE & Canh Kháng Về)...", 'success')

    # Khởi động monitor loop nếu chưa chạy
    if is_monitoring and monitor_thread is None:
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()

    while True:
        try:
            config = load_config()
            bot_token = config.get('telegram_bot_token', '')
            admin_chat_id = str(config.get('telegram_chat_id', ''))

            if not bot_token:
                time.sleep(10)
                continue

            url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
            params = {'offset': telegram_offset, 'timeout': 30}
            r = req.get(url, params=params, timeout=40)
            data = r.json()

            if data.get('ok'):
                for result in data.get('result', []):
                    telegram_offset = result['update_id'] + 1

                    # 1. XỬ LÝ CALLBACK QUERY (NÚT BẤM INLINE)
                    if 'callback_query' in result:
                        cb = result['callback_query']
                        cb_id = cb['id']
                        cb_data = cb.get('data', '')
                        chat_id = str(cb.get('message', {}).get('chat', {}).get('id', ''))
                        msg_id = cb.get('message', {}).get('message_id')

                        if cb_data.startswith('del_'):
                            uid_to_del = cb_data.split('_')[1]
                            user_uids = config.get('users', {}).get(chat_id, [])
                            new_uids = [u for u in user_uids if u.get('uid') != uid_to_del]
                            config['users'][chat_id] = new_uids
                            save_config(config)
                            req.post(f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery", json={"callback_query_id": cb_id, "text": f"✅ Đã xóa UID {uid_to_del}"})
                            req.post(f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup", json={"chat_id": chat_id, "message_id": msg_id, "reply_markup": {"inline_keyboard": [[{"text": "✅ Đã xóa UID này", "callback_data": "none"}]]}})

                        elif cb_data.startswith('swdie_'):
                            uid_sw = cb_data.split('_')[1]
                            for u in config.get('users', {}).get(chat_id, []):
                                if u.get('uid') == uid_sw:
                                    u['monitor_type'] = 'WATCH_DIE'
                            save_config(config)
                            req.post(f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery", json={"callback_query_id": cb_id, "text": f"🔴 Đã chuyển sang Canh DIE cho UID {uid_sw}"})

                        elif cb_data.startswith('swrevive_'):
                            uid_sw = cb_data.split('_')[1]
                            for u in config.get('users', {}).get(chat_id, []):
                                if u.get('uid') == uid_sw:
                                    u['monitor_type'] = 'WATCH_REVIVE'
                            save_config(config)
                            req.post(f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery", json={"callback_query_id": cb_id, "text": f"🟢 Đã chuyển sang Canh Kháng Về cho UID {uid_sw}"})

                        continue

                    # 2. XỬ LÝ TIN NHẮN VĂN BẢN HOẶC FILE
                    message = result.get('message', {})
                    text = message.get('text', '').strip()
                    chat_id = str(message.get('chat', {}).get('id', ''))

                    if not chat_id:
                        continue

                    # Kiểm tra quyền / Hạn sử dụng
                    allowed_users = config.setdefault('allowed_users', {})
                    if isinstance(allowed_users, list):
                        allowed_users = {str(uid): "permanent" for uid in allowed_users}
                        config['allowed_users'] = allowed_users

                    if not allowed_users and admin_chat_id:
                        allowed_users[admin_chat_id] = "permanent"
                        save_config(config)

                    # Xử lý Start & Referral
                    if text.startswith('/start'):
                        parts = text.split()
                        if len(parts) > 1 and parts[1].startswith('ref_'):
                            referrer = parts[1].replace('ref_', '').strip()
                            if referrer and referrer != chat_id:
                                create_pending_referral(referrer, chat_id)

                    # Lệnh cấp quyền Admin
                    if text.startswith('/allow '):
                        if chat_id != admin_chat_id:
                            send_telegram(bot_token, chat_id, "⛔ Chỉ Admin mới có quyền cấp phép.")
                        else:
                            parts = text.split(' ')
                            if len(parts) >= 2:
                                new_id = parts[1].strip()
                                days = int(parts[2].strip()) if len(parts) >= 3 and parts[2].strip().isdigit() else None
                                exp_text, _ = extend_access(allowed_users, new_id, days) if days else ("permanent", False)
                                if not days: allowed_users[new_id] = "permanent"
                                config['allowed_users'] = allowed_users
                                config.setdefault('users', {}).setdefault(new_id, [])
                                save_config(config)
                                send_telegram(bot_token, chat_id, f"✅ Đã cấp quyền cho ID: <code>{new_id}</code> (Hạn: {exp_text})")
                                send_telegram(bot_token, new_id, f"🎉 Bạn đã được cấp quyền sử dụng Bot! Hãy gõ /menu để bắt đầu.")
                        continue

                    # Kiểm tra quyền người dùng
                    if chat_id not in allowed_users:
                        send_telegram(bot_token, chat_id, f"⛔ Bạn chưa được cấp quyền sử dụng Bot.\n👉 Gửi ID của bạn: <code>{chat_id}</code> cho Admin để kích hoạt.")
                        continue

                    # Kiểm tra hết hạn
                    exp_date_str = allowed_users.get(chat_id)
                    if exp_date_str and exp_date_str != "permanent":
                        try:
                            if now_vn() > datetime.strptime(exp_date_str, '%Y-%m-%d %H:%M:%S'):
                                send_telegram(bot_token, chat_id, f"⏰ Gói cước đã hết hạn vào {exp_date_str}. Vui lòng liên hệ Admin để gia hạn.")
                                continue
                        except Exception: pass

                    user_uids = config.setdefault('users', {}).setdefault(chat_id, [])

                    # ---------------------------------------------------------
                    # CÁC LỆNH MENU & CHỨC NĂNG
                    # ---------------------------------------------------------
                    if text in ('/start', '/menu', 'menu', 'dashboard'):
                        live_cnt = sum(1 for u in user_uids if uid_alive_status.get(u['uid']) is True)
                        die_cnt = sum(1 for u in user_uids if uid_alive_status.get(u['uid']) is False)
                        cf_st = "🟢 Sẵn sàng" if config.get('cf_worker_url') else "⚪ Chưa gắn"

                        menu_msg = (
                            f"🤖 <b>HỆ THỐNG GIÁM SÁT FACEBOOK UID 24/7</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"📊 <b>Tổng theo dõi:</b> <b>{len(user_uids)} UID</b>\n"
                            f"🟢 <b>Nick Live:</b> <b>{live_cnt}</b> | 🔴 <b>Nick DIE:</b> <b>{die_cnt}</b>\n"
                            f"⚡ <b>Cổng Cloudflare Edge:</b> {cf_st}\n"
                            f"⏰ <b>Hạn dùng:</b> <code>{exp_date_str or 'Vĩnh viễn'}</code>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"👉 <b>Chọn tính năng bên dưới hoặc gửi lệnh:</b>"
                        )
                        rm = {
                            "inline_keyboard": [
                                [{"text": "🔴 Thêm Canh DIE (Nick Live)", "callback_data": "none"}, {"text": "🟢 Thêm Canh Kháng (Nick DIE)", "callback_data": "none"}],
                                [{"text": "📋 Danh Sách UID", "callback_data": "none"}, {"text": "⚡ Quét Ngay Tức Thì", "callback_data": "none"}],
                                [{"text": "🎁 Giới Thiệu Nhận +15 Ngày", "callback_data": "none"}]
                            ]
                        }
                        send_telegram(bot_token, chat_id, menu_msg, rm)

                    # 1. Thêm Nick Canh DIE: /add_live hoặc /live
                    elif text.startswith('/add_live ') or text.startswith('/live '):
                        parts = text.split(' ', 2)
                        if len(parts) >= 2:
                            uid = parts[1].strip()
                            name = parts[2].strip() if len(parts) > 2 else uid
                            if any(u['uid'] == uid for u in user_uids):
                                send_telegram(bot_token, chat_id, f"⚠️ UID <code>{uid}</code> đã có trong danh sách.")
                            else:
                                user_uids.append({
                                    'uid': uid, 'name': name, 'added_at': now_vn().strftime('%Y-%m-%d %H:%M:%S'),
                                    'monitor_type': 'WATCH_DIE'
                                })
                                save_config(config)
                                reward_pending_referral(chat_id, config, bot_token, admin_chat_id)
                                st = check_uid_alive(uid)
                                uid_alive_status[uid] = (st == 'alive')
                                photo_url = f"https://graph.facebook.com/{uid}/picture?type=large&redirect=true"
                                msg = (
                                    f"🔴 <b>ĐÃ THÊM CANH DIE:</b> <code>{uid}</code>\n"
                                    f"👤 <b>Tên:</b> {name}\n"
                                    f"🔍 <b>Trạng thái hiện tại:</b> {'🟢 LIVE' if st == 'alive' else '🔴 DIE'}\n"
                                    f"⚡ <i>Hệ thống sẽ cảnh báo ngay khi nick này bị DIE / Checkpoint!</i>"
                                )
                                rm = {"inline_keyboard": [[{"text": "🗑️ Xóa UID", "callback_data": f"del_{uid}"}]]}
                                send_telegram_photo(bot_token, chat_id, photo_url, msg, rm)
                        else:
                            send_telegram(bot_token, chat_id, "👉 Cú pháp chuẩn: <code>/live 100085368620445 ViaAds01</code>")

                    # 2. Thêm Nick Canh Kháng (Sống lại): /add hoặc /add_die hoặc /khang
                    elif text.startswith('/add ') or text.startswith('/add_die ') or text.startswith('/khang '):
                        parts = text.split(' ', 2)
                        if len(parts) >= 2:
                            uid = parts[1].strip()
                            name = parts[2].strip() if len(parts) > 2 else uid
                            if any(u['uid'] == uid for u in user_uids):
                                send_telegram(bot_token, chat_id, f"⚠️ UID <code>{uid}</code> đã có trong danh sách.")
                            else:
                                user_uids.append({
                                    'uid': uid, 'name': name, 'added_at': now_vn().strftime('%Y-%m-%d %H:%M:%S'),
                                    'monitor_type': 'WATCH_REVIVE'
                                })
                                save_config(config)
                                reward_pending_referral(chat_id, config, bot_token, admin_chat_id)
                                st = check_uid_alive(uid)
                                uid_alive_status[uid] = (st == 'alive')
                                photo_url = f"https://graph.facebook.com/{uid}/picture?type=large&redirect=true"
                                msg = (
                                    f"🟢 <b>ĐÃ THÊM CANH KHÁNG VỀ:</b> <code>{uid}</code>\n"
                                    f"👤 <b>Tên:</b> {name}\n"
                                    f"🔍 <b>Trạng thái hiện tại:</b> {'🟢 LIVE (Đã sống sẵn)' if st == 'alive' else '🔴 DIE (Đang dính 956/282)'}\n"
                                    f"⚡ <i>Hệ thống sẽ reo chuông ngay khi nick được Meta mở khóa!</i>"
                                )
                                rm = {"inline_keyboard": [[{"text": "🗑️ Xóa UID", "callback_data": f"del_{uid}"}]]}
                                send_telegram_photo(bot_token, chat_id, photo_url, msg, rm)
                        else:
                            send_telegram(bot_token, chat_id, "👉 Cú pháp chuẩn: <code>/add 100085368620445 KhachNguyenVanA</code>")

                    # 3. Xem danh sách: /list
                    elif text == '/list':
                        if not user_uids:
                            send_telegram(bot_token, chat_id, "📭 Danh sách của bạn đang trống. Hãy dùng /add hoặc /live để thêm.")
                        else:
                            lines = ["📋 <b>DANH SÁCH THEO DÕI CỦA BẠN:</b>\n"]
                            for u in user_uids:
                                uid = u['uid']
                                st = uid_alive_status.get(uid)
                                st_icon = '🟢' if st is True else ('🔴' if st is False else '⚪')
                                mode_tag = '[Canh DIE]' if u.get('monitor_type') == 'WATCH_DIE' else '[Canh Kháng]'
                                lines.append(f"{st_icon} <code>{uid}</code> | {u.get('name')} <i>{mode_tag}</i>")
                            lines.append(f"\n📊 <b>Tổng: {len(user_uids)} UID</b>")
                            send_telegram(bot_token, chat_id, "\n".join(lines))

                    # 4. Quét toàn bộ ngay lập tức: /checkall
                    elif text in ('/checkall', 'checkall'):
                        if not user_uids:
                            send_telegram(bot_token, chat_id, "📭 Danh sách đang trống.")
                        else:
                            send_telegram(bot_token, chat_id, f"⏳ Đang quét kiểm tra {len(user_uids)} UID bằng Edge Scanner...")
                            uids_arr = [u['uid'] for u in user_uids]
                            cf_url = config.get('cf_worker_url', '')
                            batch_res = check_uids_via_cf_worker(uids_arr, cf_url) if cf_url else None
                            alive_c, dead_c = 0, 0

                            for u in user_uids:
                                uid = u['uid']
                                if batch_res and uid in batch_res:
                                    st = batch_res[uid]['status']
                                else:
                                    st = check_uid_alive(uid)
                                if st == 'alive':
                                    uid_alive_status[uid] = True
                                    alive_c += 1
                                else:
                                    uid_alive_status[uid] = False
                                    dead_c += 1

                            send_telegram(bot_token, chat_id, f"✅ <b>KẾT QUẢ QUÉT:</b>\n🟢 Live: <b>{alive_c}</b>\n🔴 DIE: <b>{dead_c}</b>\n📋 Gõ /list để xem chi tiết.")

                    # 5. Xóa UID: /del
                    elif text.startswith('/del '):
                        uid_del = text.split(' ', 1)[1].strip()
                        config['users'][chat_id] = [u for u in user_uids if u.get('uid') != uid_del]
                        save_config(config)
                        send_telegram(bot_token, chat_id, f"🗑️ Đã xóa UID <code>{uid_del}</code> khỏi danh sách.")

                    # 6. Cấu hình Cloudflare Worker URL: /set_worker <url>
                    elif text.startswith('/set_worker '):
                        if chat_id != admin_chat_id:
                            send_telegram(bot_token, chat_id, "⛔ Chỉ Admin mới có quyền cấu hình Worker.")
                        else:
                            w_url = text.split(' ', 1)[1].strip()
                            config['cf_worker_url'] = w_url
                            save_config(config)
                            send_telegram(bot_token, chat_id, f"✅ Đã gắn Cloudflare Worker: <code>{w_url}</code>\nTừ giờ hệ thống sẽ quét qua Edge Cloudflare siêu tốc!")

                    # 7. Giới thiệu bạn bè: /ref, /gioithieu
                    elif text in ('/ref', '/gioithieu'):
                        b_name = get_bot_username(bot_token)
                        ref_link = f"https://t.me/{b_name}?start=ref_{chat_id}"
                        tot, rwd, pnd = referral_stats(chat_id)
                        send_telegram(bot_token, chat_id, (
                            f"🎁 <b>CHƯƠNG TRÌNH GIỚI THIỆU BẠN BÈ (+15 NGÀY VIP)</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🔗 <b>Link riêng của bạn:</b>\n<code>{ref_link}</code>\n\n"
                            f"📊 <b>Thống kê:</b>\n"
                            f"• Tổng mời: <b>{tot}</b>\n"
                            f"• Đã nhận thưởng: <b>{rwd}</b> (+{rwd*15} ngày)\n"
                            f"• Đang chờ duyệt: <b>{pnd}</b>"
                        ))

                    # 8. Video Downloader (TikTok & FB)
                    elif is_tiktok_url(text):
                        send_telegram(bot_token, chat_id, "⏳ Đang lấy video TikTok không logo...")
                        v_url, err = download_tiktok_video(text)
                        if v_url:
                            send_telegram(bot_token, chat_id, f"✅ <b>Video TikTok Không Logo:</b>\n👉 <a href='{v_url}'>Bấm vào đây để xem/tải</a>")
                        else:
                            send_telegram(bot_token, chat_id, f"❌ Lỗi: {err}")

                    elif is_facebook_video_url(text):
                        send_telegram(bot_token, chat_id, "⏳ Đang xử lý video Facebook...")
                        vp, fsz, err = download_fb_video(text)
                        if vp:
                            send_telegram(bot_token, chat_id, "✅ Đã tải xong video Facebook!")
                            try: os.remove(vp)
                            except: pass
                        else:
                            send_telegram(bot_token, chat_id, f"❌ Lỗi: {err}")

                    else:
                        send_telegram(bot_token, chat_id, (
                            f"🤖 <b>HỆ THỐNG GIÁM SÁT UID FACEBOOK</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"👉 <b>Canh DIE (Nick Live):</b> <code>/live 100085368620445 Via01</code>\n"
                            f"👉 <b>Canh Kháng (Nick DIE):</b> <code>/add 100085368620445 KhachA</code>\n"
                            f"👉 <b>Xem danh sách:</b> <code>/list</code>\n"
                            f"👉 <b>Quét ngay:</b> <code>/checkall</code>\n"
                            f"👉 <b>Xóa UID:</b> <code>/del 100085368620445</code>\n"
                            f"👉 <b>Mời bạn nhận +15 ngày:</b> <code>/ref</code>"
                        ))

            time.sleep(1)
        except Exception as e:
            add_log(f"Lỗi Telegram polling: {e}", 'error')
            time.sleep(4)

# ============================================================
# FLASK WEB SERVER (CHO RENDER & UPTIMEROBOT 24/7)
# ============================================================
@app.route('/')
@app.route('/health')
@app.route('/ping')
def index():
    return jsonify({
        'status': 'online',
        'service': 'FB UID Live/Die Monitor Telegram Bot',
        'time_vn': now_vn().strftime('%Y-%m-%d %H:%M:%S'),
        'is_monitoring': is_monitoring
    }), 200

@app.route('/api/status')
def api_status():
    config = load_config()
    total_uids = sum(len(uids) for uids in config.get('users', {}).values())
    return jsonify({
        'total_users': len(config.get('users', {})),
        'total_uids': total_uids,
        'logs_count': len(logs)
    })

# Khởi chạy luồng Telegram Bot khi chạy app
bot_thread = threading.Thread(target=telegram_bot_polling, daemon=True)
bot_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
