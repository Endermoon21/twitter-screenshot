#!/usr/bin/env python3
"""
Twitter Screenshot App with Authentication
- Uses Playwright for reliable screenshot capture
- Supports Twitter login via exported cookies
- Batch queue with real-time SSE progress
- ZIP download support
"""

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

from playwright.sync_api import sync_playwright

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

    cookies = auth.get("cookies", [])
    has_auth_token = any(c.get("name") == "auth_token" for c in cookies)
    has_ct0 = any(c.get("name") == "ct0" for c in cookies)

    return {
        "authenticated": has_auth_token or has_ct0,
        "cookie_count": len(cookies),
        "timestamp": auth.get("timestamp"),
    }


def convert_to_playwright_storage(auth_data):
    """Convert our auth format to Playwright's storage_state format"""
    if not auth_data or not auth_data.get("cookies"):
        return None

    # Convert cookies to Playwright format
    cookies = []
    for cookie in auth_data.get("cookies", []):
        pw_cookie = {
            "name": cookie["name"],
            "value": cookie["value"],
            "domain": cookie.get("domain", ".x.com"),
            "path": cookie.get("path", "/"),
            "secure": cookie.get("secure", True),
            "httpOnly": cookie.get("httpOnly", False),
            "sameSite": cookie.get("sameSite", "Lax"),
        }
        # Add expiry if present
        if cookie.get("expires") or cookie.get("expirationDate"):
            pw_cookie["expires"] = cookie.get("expires") or cookie.get("expirationDate")
        cookies.append(pw_cookie)

    # Build storage state
    storage_state = {
        "cookies": cookies,
        "origins": []
    }

    # Add localStorage if present
    if auth_data.get("localStorage"):
        local_storage_items = []
        for key, value in auth_data["localStorage"].items():
            local_storage_items.append({"name": key, "value": value})

        storage_state["origins"].append({
            "origin": "https://x.com",
            "localStorage": local_storage_items
        })

    return storage_state

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

def capture_tweet_playwright(url, parsed, theme="dark", hide_metrics=False, width=550):
    """Capture tweet using Playwright with authentication"""
    try:
        # Load auth data
        auth = load_auth()
        storage_state = convert_to_playwright_storage(auth)

        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(
                headless=False,  # Xvfb provides display
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                ]
            )

            # Create context with storage state if available
            context_args = {
                "viewport": {"width": width, "height": 1200},
                "device_scale_factor": 2,
                "color_scheme": "dark" if theme == "dark" else "light",
            }

            if storage_state:
                logger.info(f"Applying {len(storage_state['cookies'])} cookies...")
                context_args["storage_state"] = storage_state

            context = browser.new_context(**context_args)
            page = context.new_page()

            # Navigate to tweet
            logger.info(f"Loading tweet: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for page to load
            page.wait_for_timeout(3000)

            # Log the actual URL after navigation (check for redirects)
            actual_url = page.url
            logger.info(f"Actual URL after navigation: {actual_url}")

            # Debug: Check page content for errors and retry if needed
            page_title = page.title()
            has_error = page.evaluate('document.body.innerText.includes("Something went wrong")')
            logger.info(f"Page title: {page_title}, Has error: {has_error}")

            # If Twitter shows error, try clicking Retry or reloading
            if has_error:
                logger.warning("Twitter error detected, attempting retry...")
                # Try clicking the Retry button
                retry_btn = page.query_selector('button:has-text("Retry"), [role="button"]:has-text("Retry")')
                if retry_btn:
                    retry_btn.click()
                    page.wait_for_timeout(3000)
                else:
                    # Just reload
                    page.reload()
                    page.wait_for_timeout(3000)

                # Check again
                has_error = page.evaluate('document.body.innerText.includes("Something went wrong")')
                if has_error:
                    logger.error("Twitter still showing error after retry")


            # Check if we hit the login wall
            body_text = page.evaluate('document.body.innerText')

            if "Log in to X" in body_text or ("Log in" in body_text and "Sign up" in body_text and "article" not in page.content()):
                logger.warning("Login wall detected - trying to reload...")
                page.reload()
                page.wait_for_timeout(3000)
                body_text = page.evaluate('document.body.innerText')

                if "Log in to X" in body_text or ("Log in" in body_text and "Sign up" in body_text and "article" not in page.content()):
                    logger.error("Login wall still present - auth may be expired")
                    browser.close()
                    return {"success": False, "error": "Login required - please update your authentication"}

            # Try to find tweet element
            tweet_found = False
            for i in range(10):
                try:
                    has_article = page.evaluate('!!document.querySelector("article")')
                    if has_article:
                        tweet_found = True
                        logger.info(f"Tweet found after {i+1}s")
                        break
                except:
                    pass
                page.wait_for_timeout(1000)

            if not tweet_found:
                browser.close()
                return {"success": False, "error": "Could not find tweet - it may not exist or be private"}

            # Wait for images to load
            page.evaluate('''() => {
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

            # NOTE: "Show more" click code removed - it was causing quote tweet content to disappear
            # Wait a moment for content to settle
            page.wait_for_timeout(300)

            # Hide metrics if requested
            if hide_metrics:
                page.evaluate('''() => {
                    document.querySelectorAll('[data-testid="like"], [data-testid="retweet"], [data-testid="reply"]')
                        .forEach(el => {
                            const parent = el.closest('[role="group"]');
                            if (parent) parent.style.display = 'none';
                        });
                }''')

            # Remove UI clutter for cleaner screenshot
            # Pass the username from URL to find the correct article
            target_username = parsed['username'].lower()
            page.evaluate('''(targetUser) => {
                // Hide scrollbars and set consistent background
                document.documentElement.style.overflow = 'hidden';
                document.body.style.overflow = 'hidden';
                document.documentElement.style.scrollbarWidth = 'none';
                document.body.style.scrollbarWidth = 'none';

                // Set background color based on theme (prevents black bars)
                const isDark = document.documentElement.style.colorScheme === 'dark' ||
                               window.matchMedia('(prefers-color-scheme: dark)').matches ||
                               document.body.style.backgroundColor.includes('0, 0, 0');
                const bgColor = isDark ? 'rgb(0, 0, 0)' : 'rgb(255, 255, 255)';
                document.body.style.backgroundColor = bgColor;
                document.documentElement.style.backgroundColor = bgColor;

                const style = document.createElement('style');
                style.textContent = `
                    ::-webkit-scrollbar { display: none !important; }
                    /* Ensure quoted tweets expand fully - but don't change display */
                    [data-testid="card.wrapper"],
                    [data-testid="quoteTweet"],
                    [data-testid="tweetText"] {
                        max-height: none !important;
                        overflow: visible !important;
                        -webkit-line-clamp: unset !important;
                    }
                `;
                document.head.appendChild(style);
                // Hide UI elements using CSS (safer than removing)
                const hideEl = (el) => { if (el) el.style.display = 'none'; };
                hideEl(document.querySelector('[data-testid="BottomBar"]'));
                hideEl(document.querySelector('[data-testid="sidebarColumn"]'));
                hideEl(document.querySelector('header[role="banner"]'));
                document.querySelectorAll('[role="dialog"]').forEach(hideEl);
                document.querySelectorAll('[aria-label="Timeline: Trending now"]').forEach(hideEl);
                document.querySelectorAll('[data-testid="sheetDialog"]').forEach(hideEl);

                // Hide the "< Post" navigation bar at top of tweet page
                const backButton = document.querySelector('[aria-label="Back"]');
                if (backButton) {
                    let el = backButton;
                    for (let i = 0; i < 10 && el; i++) {
                        el = el.parentElement;
                        if (el && (el.getAttribute('data-testid') === 'primaryColumn' ||
                                   el.tagName === 'MAIN')) {
                            break;
                        }
                        if (el && el.textContent && el.textContent.includes('Post')) {
                            el.style.display = 'none';
                            break;
                        }
                    }
                }

                // Hide the "More Tweets" section and replies
                // IMPORTANT: Find the article matching the URL's username (for quote tweets)
                const articles = document.querySelectorAll('article');
                let targetArticle = null;

                // First, try to find article with matching username
                for (let idx = 0; idx < articles.length; idx++) {
                    const art = articles[idx];
                    const userNameEl = art.querySelector('[data-testid="User-Name"]');
                    if (userNameEl) {
                        const links = userNameEl.querySelectorAll('a[href*="/"]');
                        for (const link of links) {
                            const href = link.getAttribute('href') || '';
                            const username = href.replace('/', '').toLowerCase();
                            if (username === targetUser.toLowerCase()) {
                                targetArticle = art;
                                break;
                            }
                        }
                    }
                    if (targetArticle) break;
                }

                // If no match found, fall back to first article
                if (!targetArticle && articles.length > 0) {
                    targetArticle = articles[0];
                }

                // Hide all other articles (don't remove to avoid React crash)
                for (const art of articles) {
                    if (art !== targetArticle) {
                        art.style.display = 'none';
                    }
                }

                // Use the target article for further processing
                const article = targetArticle;
                if (article) {
                    // Hide "Relevant" dropdown and "View quotes" links
                    article.querySelectorAll('span').forEach(span => {
                        const text = span.textContent.trim();
                        if (text === 'Relevant' || text === 'View quotes' || text.includes('View quotes')) {
                            let el = span;
                            for (let i = 0; i < 6 && el; i++) {
                                el = el.parentElement;
                                if (el && (el.getAttribute('role') === 'button' || el.tagName === 'A')) {
                                    el.style.display = 'none';
                                    break;
                                }
                            }
                        }
                    });

                    // Hide content after the LAST engagement bar (replies section)
                    const groups = article.querySelectorAll('[role="group"]');
                    if (groups.length > 0) {
                        const lastGroup = groups[groups.length - 1];
                        let next = lastGroup.parentElement?.nextElementSibling;
                        while (next) {
                            next.style.display = 'none';
                            next = next.nextElementSibling;
                        }
                    }
                }
            }''', target_username)

            page.wait_for_timeout(500)

            # Position article for screenshot and get clip bounds
            clip = page.evaluate('''() => {
                const article = document.querySelector('article');
                if (!article) return null;

                // Remove top padding/margin from containers
                const primaryColumn = document.querySelector('[data-testid="primaryColumn"]');
                if (primaryColumn) {
                    primaryColumn.style.paddingTop = '0';
                    primaryColumn.style.marginTop = '0';
                }

                // Walk up and remove padding
                let container = article.parentElement;
                for (let i = 0; i < 10 && container && container !== document.body; i++) {
                    container.style.paddingTop = '0';
                    container.style.marginTop = '0';
                    container = container.parentElement;
                }

                // Add spacer above article to ensure profile pic is visible
                // This is needed because removing the header puts article at y=0
                const spacer = document.createElement('div');
                spacer.style.height = '50px';
                spacer.style.backgroundColor = 'inherit';
                article.parentElement.insertBefore(spacer, article);

                // Scroll to top
                window.scrollTo(0, 0);

                // Get position after adding spacer
                let rect = article.getBoundingClientRect();

                // IMPORTANT: Find the actual top by checking profile pic, which is often above article rect
                let topY = rect.top;

                // Check avatar
                const avatar = article.querySelector('[data-testid="Tweet-User-Avatar"]');
                if (avatar) {
                    const avatarRect = avatar.getBoundingClientRect();
                    topY = Math.min(topY, avatarRect.top);
                    console.log('Avatar top:', avatarRect.top, 'Article top:', rect.top);
                }

                // Also check for any profile images
                const profileImg = article.querySelector('img[src*="profile_images"]');
                if (profileImg) {
                    const imgRect = profileImg.getBoundingClientRect();
                    topY = Math.min(topY, imgRect.top);
                }

                // Check user name element too
                const userName = article.querySelector('[data-testid="User-Name"]');
                if (userName) {
                    const nameRect = userName.getBoundingClientRect();
                    topY = Math.min(topY, nameRect.top);
                }

                // Find the BOTTOMMOST engagement bar (main tweet's, not embedded tweet's)
                let bottomY = rect.bottom;
                const groups = article.querySelectorAll('[role="group"]');
                let maxGroupBottom = 0;

                // Find ALL engagement bars and use the one with the LARGEST Y (bottommost)
                for (const g of groups) {
                    if (g.querySelector('[data-testid="like"]') || g.querySelector('[data-testid="reply"]')) {
                        const gBottom = g.getBoundingClientRect().bottom;
                        if (gBottom > maxGroupBottom) {
                            maxGroupBottom = gBottom;
                        }
                    }
                }

                if (maxGroupBottom > 0) {
                    bottomY = maxGroupBottom;
                }

                // For quote tweets: ensure we capture content ABOVE the embedded tweet too
                // The main tweet's text should be included even if it's at the top
                const allTweetTexts = article.querySelectorAll('[data-testid="tweetText"]');
                if (allTweetTexts.length > 1) {
                    // Multiple tweet texts = likely a quote tweet
                    // Make sure bottomY includes all content
                    allTweetTexts.forEach(tt => {
                        const ttRect = tt.getBoundingClientRect();
                        // If any text is below our current bottom, extend
                        if (ttRect.bottom > bottomY) {
                            bottomY = ttRect.bottom + 50; // Add padding for engagement bar
                        }
                    });
                }


                // Return clip region with padding
                const topPadding = 15;
                const sidePadding = 12;
                const bottomPadding = 8;

                return {
                    x: Math.max(0, rect.left - sidePadding),
                    y: Math.max(0, topY - topPadding),
                    width: rect.width + (sidePadding * 2),
                    height: (bottomY - topY) + topPadding + bottomPadding
                };
            }''')

            page.wait_for_timeout(200)

            # Take screenshot with clip (Playwright handles scaling)
            screenshot_path = f"/tmp/tweet_{parsed['tweet_id']}_{int(time.time())}.png"


            # Use element screenshot for more reliable capture
            article_element = page.query_selector('article')
            if article_element:
                # Scroll article into view
                article_element.scroll_into_view_if_needed()
                page.wait_for_timeout(300)

                # Take screenshot of just the article element
                logger.info("Taking article element screenshot")
                article_element.screenshot(path=screenshot_path)
            elif clip and clip.get('height', 0) > 50:
                logger.info(f"Fallback to clip bounds: {clip}")
                page.screenshot(path=screenshot_path, clip=clip)
            else:
                # Fallback to full viewport
                page.screenshot(path=screenshot_path, full_page=False)

            # Read screenshot bytes
            with open(screenshot_path, 'rb') as f:
                screenshot_bytes = f.read()
            os.remove(screenshot_path)

            # Close browser
            browser.close()

            logger.info(f"Captured tweet {parsed['tweet_id']}")
            return {
                "success": True,
                "bytes": screenshot_bytes,
                "username": parsed['username'],
                "tweet_id": parsed['tweet_id']
            }

    except Exception as e:
        logger.error(f"Capture error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def capture_tweet_sync(url, parsed, theme="dark", hide_metrics=False, width=550):
    """Synchronous capture function"""
    return capture_tweet_playwright(url, parsed, theme, hide_metrics, width)

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
            total = len(SCREENSHOT_STORAGE)
            pad_width = len(str(total))  # Dynamic padding based on count
            
            for idx, (item_id, data) in enumerate(SCREENSHOT_STORAGE.items(), start=1):
                username = data.get('username', 'tweet')
                tweet_id = data.get('tweet_id', item_id)
                # Prefix with zero-padded index to maintain order
                prefix = str(idx).zfill(pad_width)
                name = f"{prefix}_tweet_{username}_{tweet_id}.png"
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
    pip install playwright
    playwright install chromium
"""

import json
import os
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: playwright not installed")
    print("Install with: pip install playwright && playwright install chromium")
    sys.exit(1)


def main():
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://x.com/login")

        print()
        print("Browser opened. Please log into Twitter.")
        print()
        input("Press Enter after you've logged in and can see your timeline...")

        # Get storage state (cookies + localStorage)
        storage = context.storage_state()

        auth_data = {
            "cookies": storage["cookies"],
            "localStorage": {},
            "timestamp": __import__("datetime").datetime.now().isoformat(),
        }

        # Extract localStorage
        for origin in storage.get("origins", []):
            if "x.com" in origin.get("origin", ""):
                for item in origin.get("localStorage", []):
                    auth_data["localStorage"][item["name"]] = item["value"]

        browser.close()

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
    main()
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
    print("Twitter Screenshot App - Playwright Edition")
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
