#!/usr/bin/env python3
"""
Twitter Screenshot App with Authentication
- Uses nodriver (undetected Chrome) for anti-bot bypass
- Supports Twitter login via exported cookies
- Batch queue with real-time SSE progress
- ZIP download support
"""

import asyncio
import json
import logging
import os
import queue
import re
import threading
import time
import uuid
import zipfile
from collections import OrderedDict
from datetime import datetime
from io import BytesIO
from pathlib import Path

from flask import (Flask, Response, jsonify, make_response, render_template,
                   request, send_file, stream_with_context)

import nodriver as uc

# =============================================================================
# Logging Configuration
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('twitter-screenshot')

# =============================================================================
# Configuration
# =============================================================================

CHROME_PATH = '/root/.cache/ms-playwright/chromium-1200/chrome-linux64/chrome'
SAVE_DIR = "/tmp/twitter_screenshots"
CONFIG_DIR = "/opt/twitter-screenshot/config"
AUTH_FILE = os.path.join(CONFIG_DIR, "auth.json")

os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)

MAX_QUEUE_SIZE = 50
MAX_SCREENSHOT_AGE = 3600

# =============================================================================
# Flask App
# =============================================================================

app = Flask(__name__, static_folder="static", template_folder="static")

SCREENSHOT_STORAGE = OrderedDict()
STORAGE_LOCK = threading.Lock()

class QueueStatus:
    PENDING = 'pending'
    PROCESSING = 'processing'
    DONE = 'done'
    ERROR = 'error'

work_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
queue_items = []
QUEUE_LOCK = threading.Lock()
queue_events = []
EVENTS_LOCK = threading.Lock()
MESSAGE_COUNTER = 0

# =============================================================================
# Auth Functions
# =============================================================================

def load_auth():
    """Load authentication data from config file"""
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load auth: {e}")
    return None


def save_auth(auth_data):
    """Save authentication data to config file"""
    try:
        with open(AUTH_FILE, 'w') as f:
            json.dump(auth_data, f, indent=2)
        logger.info("Auth saved successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to save auth: {e}")
        return False


def clear_auth():
    """Clear authentication data"""
    if os.path.exists(AUTH_FILE):
        os.remove(AUTH_FILE)
        logger.info("Auth cleared")


def get_auth_status():
    """Get current authentication status"""
    auth = load_auth()
    if not auth:
        return {"authenticated": False}

    # Check if we have the essential cookies
    cookies = auth.get("cookies", [])
    has_auth_token = any(c.get("name") == "auth_token" for c in cookies)
    has_ct0 = any(c.get("name") == "ct0" for c in cookies)

    return {
        "authenticated": has_auth_token or has_ct0,
        "cookie_count": len(cookies),
        "timestamp": auth.get("timestamp"),
    }

# =============================================================================
# Helper Functions
# =============================================================================

def generate_queue_id():
    return uuid.uuid4().hex[:8]


def cleanup_old_screenshots():
    now = time.time()
    expired = [k for k, v in SCREENSHOT_STORAGE.items()
               if now - v.get('created_at', 0) > MAX_SCREENSHOT_AGE]
    for k in expired:
        del SCREENSHOT_STORAGE[k]
    while len(SCREENSHOT_STORAGE) > MAX_QUEUE_SIZE:
        SCREENSHOT_STORAGE.popitem(last=False)


def parse_tweet_url(url):
    patterns = [
        r"(?:twitter\.com|x\.com)/(\w+)/status/(\d+)",
        r"(?:mobile\.twitter\.com|mobile\.x\.com)/(\w+)/status/(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return {"username": match.group(1), "tweet_id": match.group(2)}
    return None


def format_sse(data, event=None, msg_id=None):
    msg = ""
    if msg_id:
        msg += f"id: {msg_id}\n"
    if event:
        msg += f"event: {event}\n"
    msg += f"data: {data}\n\n"
    return msg


def broadcast_queue_update():
    global MESSAGE_COUNTER
    MESSAGE_COUNTER += 1

    with QUEUE_LOCK:
        status = {
            'type': 'queue_update',
            'items': queue_items.copy(),
            'queue_size': work_queue.qsize(),
            'storage_count': len(SCREENSHOT_STORAGE),
        }

    message = format_sse(json.dumps(status), event='queue_update', msg_id=MESSAGE_COUNTER)

    with EVENTS_LOCK:
        dead_queues = []
        for q in queue_events:
            try:
                q.put_nowait(message)
            except:
                dead_queues.append(q)
        for q in dead_queues:
            try:
                queue_events.remove(q)
            except:
                pass

# =============================================================================
# Capture Functions
# =============================================================================

async def capture_tweet_nodriver(url, parsed, theme="dark", hide_metrics=False, width=550):
    """Capture tweet using nodriver with authentication"""
    browser = None
    try:
        # Load auth data
        auth = load_auth()

        # Start browser
        browser = await uc.start(
            headless=False,  # Xvfb provides display
            browser_executable_path=CHROME_PATH,
            browser_args=[
                f'--window-size={width},1200',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--force-device-scale-factor=2',
            ]
        )

        # If we have auth, set cookies first
        if auth and auth.get("cookies"):
            logger.info("Applying saved cookies...")

            # Navigate to Twitter first to set cookies on the domain
            page = await browser.get("https://x.com")
            await page.sleep(1)

            # Set cookies
            for cookie in auth["cookies"]:
                try:
                    await browser.cookies.set(
                        name=cookie["name"],
                        value=cookie["value"],
                        domain=cookie.get("domain", ".x.com"),
                        path=cookie.get("path", "/"),
                        secure=cookie.get("secure", True),
                        http_only=cookie.get("httpOnly", False),
                    )
                except Exception as e:
                    logger.debug(f"Cookie set error: {e}")

            logger.info(f"Set {len(auth['cookies'])} cookies")

        # Navigate to tweet
        logger.info(f"Loading tweet: {url}")
        page = await browser.get(url)

        # Wait for page to load
        await page.sleep(3)

        # Check if we hit the login wall
        body_text = await page.evaluate('document.body.innerText')

        if "Log in" in body_text and "Sign up" in body_text:
            # Try refreshing with cookies applied
            logger.warning("Login wall detected, retrying...")
            await page.sleep(1)
            page = await browser.get(url)
            await page.sleep(3)
            body_text = await page.evaluate('document.body.innerText')

        if "Log in to X" in body_text or ("Log in" in body_text and "Sign up" in body_text and "article" not in await page.evaluate('document.body.innerHTML')):
            logger.error("Login wall still present - auth may be expired")
            await browser.stop()
            return {"success": False, "error": "Login required - please update your authentication"}

        # Try to find tweet element
        tweet_found = False
        for i in range(10):
            try:
                has_article = await page.evaluate('!!document.querySelector("article")')
                if has_article:
                    tweet_found = True
                    logger.info(f"Tweet found after {i+1}s")
                    break
            except:
                pass
            await page.sleep(1)

        if not tweet_found:
            await browser.stop()
            return {"success": False, "error": "Could not find tweet - it may not exist or be private"}

        # Wait for images to load
        await page.evaluate('''() => {
            return new Promise(resolve => {
                const images = document.querySelectorAll('img');
                let loaded = 0;
                const total = images.length;
                if (total === 0) { resolve(); return; }
                const checkDone = () => { if (++loaded >= total) resolve(); };
                images.forEach(img => {
                    if (img.complete) checkDone();
                    else { img.onload = checkDone; img.onerror = checkDone; }
                });
                setTimeout(resolve, 5000);
            });
        }''')

        # Apply theme
        if theme == "light":
            await page.evaluate('''() => {
                document.documentElement.style.colorScheme = 'light';
            }''')
        else:
            await page.evaluate('''() => {
                document.documentElement.style.colorScheme = 'dark';
            }''')

        # Hide metrics if requested
        if hide_metrics:
            await page.evaluate('''() => {
                document.querySelectorAll('[data-testid="like"], [data-testid="retweet"], [data-testid="reply"]')
                    .forEach(el => {
                        const parent = el.closest('[role="group"]');
                        if (parent) parent.style.display = 'none';
                    });
            }''')

        # Remove UI clutter for cleaner screenshot
        await page.evaluate('''() => {
            // Remove bottom bar
            document.querySelector('[data-testid="BottomBar"]')?.remove();

            // Remove dialogs/modals
            document.querySelectorAll('[role="dialog"]').forEach(e => e.remove());

            // Remove sidebar
            document.querySelector('[data-testid="sidebarColumn"]')?.remove();

            // Remove header
            document.querySelector('header[role="banner"]')?.remove();

            // Remove "What's happening" and other sidebars
            document.querySelectorAll('[aria-label="Timeline: Trending now"]').forEach(e => e.remove());

            // Remove login prompts
            document.querySelectorAll('[data-testid="sheetDialog"]').forEach(e => e.remove());

            // Hide the "More Tweets" section
            const articles = document.querySelectorAll('article');
            if (articles.length > 1) {
                // Keep only the first article (the main tweet)
                for (let i = 1; i < articles.length; i++) {
                    articles[i].style.display = 'none';
                }
            }
        }''')

        await page.sleep(0.5)

        # Get tweet bounds for cropping
        bounds = await page.evaluate('''() => {
            const article = document.querySelector('article');
            if (article) {
                const rect = article.getBoundingClientRect();
                return {
                    x: Math.max(0, rect.x - 10),
                    y: Math.max(0, rect.y - 10),
                    width: rect.width + 20,
                    height: rect.height + 20
                };
            }
            return null;
        }''')

        # Take screenshot
        screenshot_path = f"/tmp/tweet_{parsed['tweet_id']}_{int(time.time())}.png"
        await page.save_screenshot(screenshot_path)

        # Crop to tweet if we got bounds
        if bounds and bounds.get('height', 0) > 100:
            try:
                from PIL import Image
                img = Image.open(screenshot_path)

                scale = 2  # device scale factor
                crop_box = (
                    int(bounds['x'] * scale),
                    int(bounds['y'] * scale),
                    int((bounds['x'] + bounds['width']) * scale),
                    int((bounds['y'] + bounds['height']) * scale)
                )

                # Ensure within bounds
                crop_box = (
                    max(0, crop_box[0]),
                    max(0, crop_box[1]),
                    min(img.width, crop_box[2]),
                    min(img.height, crop_box[3])
                )

                if crop_box[2] > crop_box[0] and crop_box[3] > crop_box[1]:
                    cropped = img.crop(crop_box)
                    cropped.save(screenshot_path)
                    logger.info(f"Cropped from {img.size} to {cropped.size}")
            except Exception as e:
                logger.warning(f"Crop failed: {e}")

        # Read screenshot bytes
        with open(screenshot_path, 'rb') as f:
            screenshot_bytes = f.read()
        os.remove(screenshot_path)

        # Stop browser
        try:
            result = browser.stop()
            if asyncio.iscoroutine(result):
                await result
        except:
            pass

        logger.info(f"Captured tweet {parsed['tweet_id']}")
        return {
            "success": True,
            "bytes": screenshot_bytes,
            "username": parsed['username'],
            "tweet_id": parsed['tweet_id']
        }

    except Exception as e:
        logger.error(f"Capture error: {e}")
        if browser:
            try:
                browser.stop()
            except:
                pass
        return {"success": False, "error": str(e)}


def capture_tweet_sync(url, parsed, theme="dark", hide_metrics=False, width=550):
    """Synchronous wrapper for async capture"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(
            capture_tweet_nodriver(url, parsed, theme, hide_metrics, width)
        )
        return result
    finally:
        loop.close()

# =============================================================================
# Queue Worker
# =============================================================================

def queue_worker():
    while True:
        try:
            item = work_queue.get()

            with QUEUE_LOCK:
                for i in queue_items:
                    if i['id'] == item['id']:
                        i['status'] = QueueStatus.PROCESSING
                        break
            broadcast_queue_update()

            try:
                result = capture_tweet_sync(
                    item['url'],
                    item['parsed'],
                    theme=item.get('theme', 'dark'),
                    hide_metrics=item.get('hide_metrics', False),
                    width=item.get('width', 550)
                )

                if result['success']:
                    with STORAGE_LOCK:
                        cleanup_old_screenshots()
                        SCREENSHOT_STORAGE[item['id']] = {
                            'bytes': result['bytes'],
                            'url': item['url'],
                            'username': result['username'],
                            'tweet_id': result['tweet_id'],
                            'created_at': time.time(),
                        }
                        SCREENSHOT_STORAGE.move_to_end(item['id'])

                    with QUEUE_LOCK:
                        for i in queue_items:
                            if i['id'] == item['id']:
                                i['status'] = QueueStatus.DONE
                                i['completed_at'] = datetime.now().isoformat()
                                break
                else:
                    with QUEUE_LOCK:
                        for i in queue_items:
                            if i['id'] == item['id']:
                                i['status'] = QueueStatus.ERROR
                                i['error'] = result.get('error', 'Unknown error')
                                break

            except Exception as e:
                logger.error(f"Worker error: {e}")
                with QUEUE_LOCK:
                    for i in queue_items:
                        if i['id'] == item['id']:
                            i['status'] = QueueStatus.ERROR
                            i['error'] = str(e)
                            break

            broadcast_queue_update()
            work_queue.task_done()

        except Exception as e:
            logger.error(f"Queue error: {e}")
            time.sleep(1)


def start_queue_worker():
    worker = threading.Thread(target=queue_worker, daemon=True)
    worker.start()
    logger.info("Queue worker started")

# =============================================================================
# Routes
# =============================================================================

@app.route("/")
def index():
    response = make_response(render_template("web.html"))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/api/health")
def health():
    auth_status = get_auth_status()
    return jsonify({
        "status": "healthy",
        "queue_size": work_queue.qsize(),
        "screenshots_stored": len(SCREENSHOT_STORAGE),
        "display": os.environ.get('DISPLAY', 'not set'),
        "auth": auth_status,
    })


@app.route("/api/auth/status")
def auth_status():
    return jsonify(get_auth_status())


@app.route("/api/auth/save", methods=["POST"])
def auth_save():
    data = request.json
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400

    # Validate we have cookies
    if not data.get("cookies"):
        return jsonify({"success": False, "error": "No cookies in auth data"}), 400

    if save_auth(data):
        return jsonify({"success": True, "status": get_auth_status()})
    return jsonify({"success": False, "error": "Failed to save auth"}), 500


@app.route("/api/auth/clear", methods=["POST"])
def auth_clear():
    clear_auth()
    return jsonify({"success": True})


@app.route("/api/capture", methods=["POST"])
def capture():
    data = request.json
    url = data.get("url", "").strip()
    theme = data.get("theme", "dark")
    hide_metrics = data.get("hide_metrics", False)
    width = max(400, min(800, int(data.get("width", 550))))

    if not url:
        return jsonify({"success": False, "error": "No URL provided"})

    parsed = parse_tweet_url(url)
    if not parsed:
        return jsonify({"success": False, "error": "Invalid tweet URL"})

    result = capture_tweet_sync(url, parsed, theme, hide_metrics, width)

    if result['success']:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tweet_{parsed['username']}_{parsed['tweet_id']}_{timestamp}.png"
        filepath = os.path.join(SAVE_DIR, filename)
        with open(filepath, 'wb') as f:
            f.write(result['bytes'])
        return jsonify({"success": True, "filepath": filepath, "filename": filename})

    return jsonify(result)


@app.route("/api/queue/add", methods=["POST"])
def queue_add():
    data = request.json or {}
    urls = data.get('urls', [])
    theme = data.get('theme', 'dark')
    hide_metrics = data.get('hide_metrics', False)
    width = max(400, min(800, int(data.get('width', 550))))

    if not urls:
        return jsonify({'error': 'No URLs provided'}), 400

    if len(urls) > MAX_QUEUE_SIZE:
        return jsonify({'error': f'Maximum {MAX_QUEUE_SIZE} URLs allowed'}), 400

    added = []
    errors = []

    for url in urls:
        url = url.strip()
        if not url:
            continue

        parsed = parse_tweet_url(url)
        if not parsed:
            errors.append({'url': url, 'error': 'Invalid tweet URL'})
            continue

        item_id = generate_queue_id()
        item = {
            'id': item_id,
            'url': url,
            'parsed': parsed,
            'theme': theme,
            'hide_metrics': hide_metrics,
            'width': width,
            'status': QueueStatus.PENDING,
            'created_at': datetime.now().isoformat(),
        }

        with QUEUE_LOCK:
            queue_items.append(item)

        try:
            work_queue.put_nowait(item)
            added.append({'id': item_id, 'url': url})
        except queue.Full:
            with QUEUE_LOCK:
                queue_items.remove(item)
            errors.append({'url': url, 'error': 'Queue is full'})

    broadcast_queue_update()
    return jsonify({'added': added, 'errors': errors})


@app.route("/api/queue/status")
def queue_status():
    with QUEUE_LOCK:
        return jsonify({
            'items': queue_items.copy(),
            'queue_size': work_queue.qsize(),
            'storage_count': len(SCREENSHOT_STORAGE),
        })


@app.route("/api/queue/stream")
def queue_stream():
    def generate():
        subscriber_queue = queue.Queue(maxsize=100)
        with EVENTS_LOCK:
            queue_events.append(subscriber_queue)

        try:
            with QUEUE_LOCK:
                initial = {
                    'type': 'queue_update',
                    'items': queue_items.copy(),
                    'queue_size': work_queue.qsize(),
                    'storage_count': len(SCREENSHOT_STORAGE),
                }
            yield format_sse(json.dumps(initial), event='queue_update')

            while True:
                try:
                    message = subscriber_queue.get(timeout=30)
                    yield message
                except queue.Empty:
                    yield format_sse('ping', event='ping')

        finally:
            with EVENTS_LOCK:
                try:
                    queue_events.remove(subscriber_queue)
                except:
                    pass

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        }
    )


@app.route("/api/queue/download/<item_id>")
def queue_download(item_id):
    with STORAGE_LOCK:
        if item_id not in SCREENSHOT_STORAGE:
            return jsonify({'error': 'Screenshot not found'}), 404

        data = SCREENSHOT_STORAGE[item_id]
        screenshot_bytes = data['bytes']
        username = data.get('username', 'tweet')
        tweet_id = data.get('tweet_id', item_id)

    filename = f"tweet_{username}_{tweet_id}.png"
    return send_file(
        BytesIO(screenshot_bytes),
        mimetype='image/png',
        as_attachment=True,
        download_name=filename
    )


@app.route("/api/queue/download-all")
def queue_download_all():
    with STORAGE_LOCK:
        if not SCREENSHOT_STORAGE:
            return jsonify({'error': 'No screenshots available'}), 404

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            seen_names = set()
            for item_id, data in SCREENSHOT_STORAGE.items():
                username = data.get('username', 'tweet')
                tweet_id = data.get('tweet_id', item_id)
                base_name = f"tweet_{username}_{tweet_id}.png"

                name = base_name
                counter = 1
                while name in seen_names:
                    name = f"tweet_{username}_{tweet_id}_{counter}.png"
                    counter += 1
                seen_names.add(name)

                zf.writestr(name, data['bytes'])

        zip_buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f'twitter_screenshots_{timestamp}.zip'
    )


@app.route("/api/queue/clear", methods=["POST"])
def queue_clear():
    with QUEUE_LOCK:
        queue_items[:] = [i for i in queue_items if i['status'] == QueueStatus.PROCESSING]
        while not work_queue.empty():
            try:
                work_queue.get_nowait()
            except:
                break

    with STORAGE_LOCK:
        SCREENSHOT_STORAGE.clear()

    broadcast_queue_update()
    return jsonify({'success': True})


@app.route("/api/download/<filename>")
def download(filename):
    filepath = os.path.join(SAVE_DIR, filename)
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    return jsonify({"error": "Not found"}), 404


@app.route("/screenshots/<filename>")
def serve_screenshot(filename):
    filepath = os.path.join(SAVE_DIR, filename)
    if os.path.exists(filepath):
        return send_file(filepath, mimetype="image/png")
    return jsonify({"error": "Not found"}), 404


@app.route("/twitter_auth.py")
def serve_auth_script():
    """Serve the authentication script for download"""
    auth_script = '''#!/usr/bin/env python3
"""
Twitter Authentication Script
Run this locally to log into Twitter and export your session.
Then paste the output into the web app.

Usage:
    python twitter_auth.py

Requirements:
    pip install nodriver
"""

import asyncio
import json
import os
import sys

try:
    import nodriver as uc
except ImportError:
    print("ERROR: nodriver not installed")
    print("Install with: pip install nodriver")
    sys.exit(1)


async def main():
    print("=" * 60)
    print("Twitter Authentication Script")
    print("=" * 60)
    print()
    print("A browser window will open. Please:")
    print("1. Log into your Twitter/X account")
    print("2. Make sure you can see your timeline")
    print("3. Come back here and press Enter")
    print()
    print("Starting browser...")

    user_data_dir = os.path.expanduser("~/.twitter_screenshot_auth")
    os.makedirs(user_data_dir, exist_ok=True)

    browser = await uc.start(
        headless=False,
        user_data_dir=user_data_dir,
    )

    page = await browser.get("https://x.com/login")

    print()
    print("Browser opened. Please log into Twitter.")
    print()
    input("Press Enter after you've logged in and can see your timeline...")

    cookies = await browser.cookies.get_all()

    cookie_list = []
    for cookie in cookies:
        cookie_dict = {
            "name": cookie.name,
            "value": cookie.value,
            "domain": cookie.domain,
            "path": cookie.path,
            "secure": cookie.secure,
            "httpOnly": cookie.http_only if hasattr(cookie, 'http_only') else False,
        }
        if hasattr(cookie, 'expires') and cookie.expires:
            cookie_dict["expires"] = cookie.expires
        if hasattr(cookie, 'sameSite') and cookie.sameSite:
            cookie_dict["sameSite"] = cookie.sameSite
        cookie_list.append(cookie_dict)

    local_storage = {}
    try:
        local_storage = await page.evaluate("""() => {
            const items = {};
            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                items[key] = localStorage.getItem(key);
            }
            return items;
        }""")
    except:
        pass

    auth_data = {
        "cookies": cookie_list,
        "localStorage": local_storage,
        "userDataDir": user_data_dir,
        "timestamp": __import__("datetime").datetime.now().isoformat(),
    }

    await browser.stop()

    print()
    print("=" * 60)
    print("SUCCESS! Copy the JSON below and paste it into the web app:")
    print("=" * 60)
    print()
    print(json.dumps(auth_data, indent=2))
    print()
    print("=" * 60)

    auth_file = os.path.expanduser("~/.twitter_screenshot_auth.json")
    with open(auth_file, "w") as f:
        json.dump(auth_data, f, indent=2)
    print(f"Also saved to: {auth_file}")
    print()

    return auth_data


if __name__ == "__main__":
    asyncio.run(main())
'''
    response = make_response(auth_script)
    response.headers["Content-Type"] = "text/plain; charset=utf-8"
    response.headers["Content-Disposition"] = "attachment; filename=twitter_auth.py"
    return response


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Twitter Screenshot App - Authenticated Direct Capture")
    print("=" * 60)

    display = os.environ.get('DISPLAY')
    if not display:
        print("ERROR: DISPLAY not set. Run with DISPLAY=:99")
        import sys
        sys.exit(1)
    print(f"Display: {display}")

    # Check auth status
    auth_status = get_auth_status()
    if auth_status["authenticated"]:
        print(f"Auth: Loaded ({auth_status['cookie_count']} cookies)")
    else:
        print("Auth: Not configured - run twitter_auth.py locally to set up")

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        start_queue_worker()

    print("\nServer: http://0.0.0.0:8891")
    print("=" * 60)

    app.run(host="0.0.0.0", port=8891, debug=False, threaded=True)
