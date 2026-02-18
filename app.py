#!/usr/bin/env python3
"""
Twitter Screenshot App - Simple Syndication Embed Approach
- Uses Twitter's public embed/syndication API (no login required)
- Simpler architecture without complex threading
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
os.makedirs(SAVE_DIR, exist_ok=True)

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

async def capture_tweet_async(tweet_id: str, username: str, theme: str = "dark",
                              hide_metrics: bool = False, width: int = 550) -> dict:
    """Capture tweet using syndication embed (no login required)"""
    browser = None
    try:
        # Start browser
        browser = await uc.start(
            headless=False,
            browser_executable_path=CHROME_PATH,
            browser_args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-software-rasterizer',
                '--force-device-scale-factor=2',
                f'--window-size={width},1200',
            ]
        )

        # Use syndication embed URL
        embed_url = f"https://platform.twitter.com/embed/Tweet.html?dnt=false&id={tweet_id}&theme={theme}"
        logger.info(f"Loading: {embed_url}")

        page = await browser.get(embed_url)

        # Wait for content to load
        for i in range(15):
            await page.sleep(1)
            has_content = await page.evaluate('''() => {
                const article = document.querySelector('article');
                const tweetText = document.querySelector('[data-testid="tweetText"]');
                return !!(article || tweetText);
            }''')
            if has_content:
                logger.info(f"Content loaded after {i+1}s")
                break

        # Check for errors
        body_text = await page.evaluate('document.body.innerText')
        if "doesn't exist" in body_text or "This post is from" in body_text:
            await browser.stop()
            return {"success": False, "error": f"Tweet not available"}

        # Hide metrics if requested
        if hide_metrics:
            await page.evaluate('''() => {
                const row = document.querySelector('[role="group"]');
                if (row) row.style.display = 'none';
            }''')

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
                setTimeout(resolve, 3000); // Max 3s wait for images
            });
        }''')

        await page.sleep(0.3)

        # Get the tweet container bounds and crop to just the tweet
        bounds = await page.evaluate('''() => {
            // Find the main tweet container - it's usually the first twitter-tweet or article
            const container = document.querySelector('.twitter-tweet') ||
                             document.querySelector('twitter-widget') ||
                             document.querySelector('article') ||
                             document.querySelector('[data-tweet-id]');

            if (!container) {
                // Fallback: find the content wrapper
                const body = document.body;
                const firstChild = body.firstElementChild;
                if (firstChild) {
                    const rect = firstChild.getBoundingClientRect();
                    return { x: 0, y: 0, width: rect.width, height: rect.height + 20 };
                }
                return null;
            }

            const rect = container.getBoundingClientRect();
            return {
                x: Math.max(0, rect.x),
                y: Math.max(0, rect.y),
                width: rect.width,
                height: rect.height + 10  // Small padding
            };
        }''')

        # Take screenshot
        screenshot_path = f"/tmp/tweet_{tweet_id}_{int(time.time())}.png"

        if bounds and bounds.get('height', 0) > 50:
            # Use CDP to capture just the tweet area
            logger.info(f"Cropping to bounds: {bounds}")

            # Take full screenshot first, then we'll crop
            await page.save_screenshot(screenshot_path)

            # Crop the image using PIL
            from PIL import Image
            img = Image.open(screenshot_path)

            # Scale bounds by device pixel ratio (2x)
            scale = 2
            crop_box = (
                int(bounds['x'] * scale),
                int(bounds['y'] * scale),
                int((bounds['x'] + bounds['width']) * scale),
                int((bounds['y'] + bounds['height']) * scale)
            )

            # Ensure crop box is within image bounds
            crop_box = (
                max(0, crop_box[0]),
                max(0, crop_box[1]),
                min(img.width, crop_box[2]),
                min(img.height, crop_box[3])
            )

            cropped = img.crop(crop_box)
            cropped.save(screenshot_path)
            logger.info(f"Cropped from {img.size} to {cropped.size}")
        else:
            # Fallback to full screenshot
            await page.save_screenshot(screenshot_path)

        with open(screenshot_path, 'rb') as f:
            screenshot_bytes = f.read()
        os.remove(screenshot_path)

        # Stop browser (don't await - may not be coroutine in all nodriver versions)
        try:
            result = browser.stop()
            if asyncio.iscoroutine(result):
                await result
        except:
            pass

        logger.info(f"Captured tweet {tweet_id}")
        return {
            "success": True,
            "bytes": screenshot_bytes,
            "username": username,
            "tweet_id": tweet_id
        }

    except Exception as e:
        logger.error(f"Error: {e}")
        if browser:
            try:
                browser.stop()  # Don't await - it may not be a coroutine
            except:
                pass
        return {"success": False, "error": str(e)}


def capture_tweet_sync(tweet_id: str, username: str, theme: str = "dark",
                       hide_metrics: bool = False, width: int = 550) -> dict:
    """Synchronous wrapper"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(
            capture_tweet_async(tweet_id, username, theme, hide_metrics, width)
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
                    tweet_id=item['parsed']['tweet_id'],
                    username=item['parsed']['username'],
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
    return jsonify({
        "status": "healthy",
        "queue_size": work_queue.qsize(),
        "screenshots_stored": len(SCREENSHOT_STORAGE),
        "display": os.environ.get('DISPLAY', 'not set'),
    })


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

    result = capture_tweet_sync(
        tweet_id=parsed['tweet_id'],
        username=parsed['username'],
        theme=theme,
        hide_metrics=hide_metrics,
        width=width
    )

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


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Twitter Screenshot App - Simple Embed Approach")
    print("=" * 60)

    display = os.environ.get('DISPLAY')
    if not display:
        print("ERROR: DISPLAY not set. Run with DISPLAY=:99")
        import sys
        sys.exit(1)
    print(f"Display: {display}")

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        start_queue_worker()

    print("\nServer: http://0.0.0.0:8891")
    print("=" * 60)

    app.run(host="0.0.0.0", port=8891, debug=False, threaded=True)
