#!/usr/bin/env python3
"""
Twitter Screenshot App with Authentication
- Uses Playwright for reliable browser automation
- Supports Twitter login via cookies or auto-login
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

from playwright.async_api import async_playwright

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

CHROME_PATH = '/usr/bin/google-chrome-stable'
SAVE_DIR = "/tmp/twitter_screenshots"
CONFIG_DIR = "/opt/twitter-screenshot/config"
AUTH_FILE = os.path.join(CONFIG_DIR, "auth.json")
CREDENTIALS_FILE = os.path.join(CONFIG_DIR, "credentials.json")
PROFILE_DIR = "/opt/twitter-screenshot/config/chrome_profile"  # Persistent browser profile

os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(PROFILE_DIR, exist_ok=True)

MAX_QUEUE_SIZE = 50
MAX_SCREENSHOT_AGE = 3600
AUTO_LOGIN_LOCK = threading.Lock()
LAST_LOGIN_ATTEMPT = 0
LOGIN_COOLDOWN = 300  # 5 minutes between auto-login attempts

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


def load_credentials():
    """Load Twitter credentials for auto-login"""
    if os.path.exists(CREDENTIALS_FILE):
        try:
            with open(CREDENTIALS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
    return None


def save_credentials(username, password):
    """Save Twitter credentials for auto-login"""
    try:
        with open(CREDENTIALS_FILE, 'w') as f:
            json.dump({"username": username, "password": password}, f)
        os.chmod(CREDENTIALS_FILE, 0o600)  # Restrict permissions
        logger.info("Credentials saved")
        return True
    except Exception as e:
        logger.error(f"Failed to save credentials: {e}")
        return False


def get_auth_status():
    """Get current authentication status"""
    # Check storage_state.json first (Playwright format)
    storage_file = os.path.join(CONFIG_DIR, "storage_state.json")
    if os.path.exists(storage_file):
        try:
            with open(storage_file, 'r') as f:
                data = json.load(f)
            cookies = data.get("cookies", [])
            has_auth_token = any(c.get("name") == "auth_token" for c in cookies)
            has_ct0 = any(c.get("name") == "ct0" for c in cookies)
            if has_auth_token or has_ct0:
                return {
                    "authenticated": True,
                    "cookie_count": len(cookies),
                    "source": "storage_state.json"
                }
        except:
            pass

    # Fall back to auth.json
    auth = load_auth()
    if not auth:
        return {"authenticated": False}

    cookies = auth.get("cookies", [])
    has_auth_token = any(c.get("name") == "auth_token" for c in cookies)
    has_ct0 = any(c.get("name") == "ct0" for c in cookies)

    return {
        "authenticated": has_auth_token or has_ct0,
        "cookie_count": len(cookies),
        "source": "auth.json"
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
# Capture Functions (Playwright)
# =============================================================================

async def capture_tweet_playwright(url, parsed, theme="dark", hide_metrics=False, width=550):
    """Capture tweet using Playwright with persistent context"""
    playwright = None
    browser = None
    context = None

    try:
        playwright = await async_playwright().start()

        # Launch browser in headless mode with stealth settings
        logger.info("Starting Playwright browser...")
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-blink-features=AutomationControlled',
                '--lang=en-US',
                '--accept-lang=en-US,en',
            ]
        )

        # Create context with persistent storage and stealth settings
        # Use larger height for quote tweets
        context = await browser.new_context(
            viewport={'width': width, 'height': 2400},
            device_scale_factor=2,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
            color_scheme='dark' if theme == 'dark' else 'light',
            storage_state=os.path.join(CONFIG_DIR, "storage_state.json") if os.path.exists(os.path.join(CONFIG_DIR, "storage_state.json")) else None
        )

        # Add script to hide webdriver
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = {runtime: {}};
        """)

        # Apply saved cookies if available
        auth = load_auth()
        if auth and auth.get("cookies"):
            logger.info("Applying saved cookies...")
            cookies_to_add = []
            for cookie in auth["cookies"]:
                c = {
                    "name": cookie["name"],
                    "value": cookie["value"],
                    "domain": cookie.get("domain", ".x.com"),
                    "path": cookie.get("path", "/"),
                }
                if cookie.get("expires"):
                    c["expires"] = cookie["expires"]
                if cookie.get("secure"):
                    c["secure"] = cookie["secure"]
                if cookie.get("httpOnly"):
                    c["httpOnly"] = cookie["httpOnly"]
                cookies_to_add.append(c)
            await context.add_cookies(cookies_to_add)
            logger.info(f"Applied {len(cookies_to_add)} cookies")

        page = await context.new_page()

        # Navigate to tweet
        logger.info(f"Loading tweet: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(3000)

        # Check for login wall - but only if there's no actual tweet content
        body_text = await page.evaluate('document.body.innerText')
        logger.info(f"Page text preview: {body_text[:200]}...")

        # Check if we need to login - simply check if storage_state exists
        storage_exists = os.path.exists(os.path.join(CONFIG_DIR, "storage_state.json"))
        has_article = await page.query_selector('article')

        if not storage_exists:
            logger.warning("Login wall detected - attempting auto-login...")

            login_success = await perform_auto_login_playwright(page)

            if login_success:
                logger.info("Auto-login succeeded, retrying tweet capture...")
                # Save storage state for future use
                await context.storage_state(path=os.path.join(CONFIG_DIR, "storage_state.json"))

                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(2000)
                body_text = await page.evaluate('document.body.innerText')

                if "Log in to X" in body_text:
                    await browser.close()
                    await playwright.stop()
                    return {"success": False, "error": "Auto-login failed - check credentials"}
            else:
                await browser.close()
                await playwright.stop()
                return {"success": False, "error": "Login required - set credentials via /api/credentials"}

        # Wait for tweet article
        try:
            await page.wait_for_selector('article', timeout=15000)
            logger.info("Tweet found")
        except Exception as e:
            # Debug: log what we see on the page
            body_text = await page.evaluate('document.body.innerText')
            logger.error(f"No article found. Page text preview: {body_text[:500]}")
            current_url = page.url
            logger.error(f"Current URL: {current_url}")
            await browser.close()
            await playwright.stop()
            return {"success": False, "error": f"Could not find tweet. URL: {current_url}"}

        # Check for quote tweet - if found, get full text and inject it back
        await page.wait_for_timeout(2000)  # Let quote tweet load
        try:
            current_tweet_id = parsed['tweet_id']
            original_url = url

            quote_url = await page.evaluate('''(currentTweetId) => {
                const article = document.querySelector('article');
                if (!article) return null;

                function getDifferentTweetUrl(href) {
                    if (!href || !href.includes('/status/')) return null;
                    const match = href.match(/(\\/[^\\/]+\\/status\\/(\\d+))/);
                    if (!match) return null;
                    const basePath = match[1];
                    const tweetId = match[2];
                    if (tweetId === currentTweetId) return null;
                    if (href.includes('/analytics') || href.includes('/retweets') ||
                        href.includes('/quotes') || href.includes('/likes')) return null;
                    return 'https://x.com' + basePath;
                }

                // Find quote tweet URL
                const allStatusLinks = article.querySelectorAll('a[href*="/status/"]');
                for (const link of allStatusLinks) {
                    const url = getDifferentTweetUrl(link.getAttribute('href'));
                    if (url) {
                        const isInTweetText = link.closest('[data-testid="tweetText"]');
                        if (!isInTweetText) return url;
                    }
                }
                return null;
            }''', current_tweet_id)

            logger.info(f"Quote detection result: {quote_url}")

            if quote_url:
                logger.info(f"Found quote tweet, fetching full text from: {quote_url}")

                # Navigate to quote tweet to get full text
                await page.goto(quote_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(3000)
                await page.wait_for_selector('article', timeout=15000)

                # Extract the full tweet text
                full_quote_text = await page.evaluate('''() => {
                    const article = document.querySelector('article');
                    if (!article) return null;
                    const tweetText = article.querySelector('[data-testid="tweetText"]');
                    return tweetText ? tweetText.innerText : null;
                }''')

                logger.info(f"Got full quote text: {full_quote_text[:100] if full_quote_text else 'None'}...")

                # Go back to original tweet
                await page.goto(original_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(3000)
                await page.wait_for_selector('article', timeout=15000)

                # Inject the full text into the quote preview
                if full_quote_text:
                    injected = await page.evaluate('''(fullText) => {
                        const article = document.querySelector('article');
                        if (!article) return false;

                        // Find the quote container - it's usually a div with role="link" that's not the main tweet
                        const mainTweetText = article.querySelector('[data-testid="tweetText"]');
                        const quoteContainers = article.querySelectorAll('div[role="link"]');

                        for (const container of quoteContainers) {
                            // Skip if this contains the main tweet text
                            if (mainTweetText && container.contains(mainTweetText)) continue;

                            // Look for text elements that look like tweet content
                            const textElements = container.querySelectorAll('span[dir="ltr"], div[dir="ltr"], [lang]');
                            for (const el of textElements) {
                                const text = el.innerText || '';
                                // Skip short text (usernames, dates, etc.)
                                if (text.length < 30) continue;
                                // Skip if it contains the full text already
                                if (text.length > fullText.length) continue;

                                // This is likely the quote text container
                                console.log('Found quote text element with:', text.substring(0, 50));

                                // Create a new element with full text
                                const newEl = document.createElement('div');
                                newEl.innerText = fullText;
                                newEl.style.whiteSpace = 'pre-wrap';
                                newEl.style.wordBreak = 'break-word';
                                newEl.style.fontSize = getComputedStyle(el).fontSize;
                                newEl.style.fontFamily = getComputedStyle(el).fontFamily;
                                newEl.style.color = getComputedStyle(el).color;
                                newEl.style.lineHeight = getComputedStyle(el).lineHeight;

                                // Replace the old element
                                el.parentElement.replaceChild(newEl, el);

                                console.log('Replaced quote text with full text');
                                return true;
                            }
                        }
                        return false;
                    }''', full_quote_text)

                    if injected:
                        logger.info("Successfully injected full quote text")
                    else:
                        logger.warning("Could not find quote text element to inject")

                    logger.info("Injected full quote text into original tweet")
                    await page.wait_for_timeout(500)

        except Exception as e:
            logger.warning(f"Quote text injection failed: {e}")

        # Scroll down to trigger lazy loading of quote tweets
        await page.evaluate('''() => {
            const article = document.querySelector('article');
            if (article) {
                article.scrollIntoView({behavior: 'instant', block: 'start'});
                window.scrollBy(0, 500);  // Scroll down a bit
            }
        }''')
        await page.wait_for_timeout(1000)

        # Scroll back up
        await page.evaluate('window.scrollTo(0, 0)')
        await page.wait_for_timeout(500)

        # Wait for images
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

        # Auto-translate tweets if translation button exists
        try:
            await page.wait_for_timeout(1500)  # Let page fully settle

            # Use Playwright's get_by_text for reliable text matching
            translate_locator = page.get_by_text("Show translation", exact=True)
            if await translate_locator.count() > 0:
                logger.info("Found 'Show translation' button, clicking...")
                await translate_locator.first.click()
                await page.wait_for_timeout(4000)

                # Check if translation failed - retry up to 2 times
                for retry in range(2):
                    error_text = await page.evaluate('document.body.innerText')
                    if "Unable to fetch" in error_text or "unable to" in error_text.lower():
                        logger.info(f"Translation failed, retry {retry + 1}...")
                        # Look for retry button or click translate again
                        retry_btn = page.get_by_text("Retry", exact=True)
                        if await retry_btn.count() > 0:
                            await retry_btn.first.click()
                        else:
                            # Try clicking Show translation again
                            show_btn = page.get_by_text("Show translation", exact=True)
                            if await show_btn.count() > 0:
                                await show_btn.first.click()
                        await page.wait_for_timeout(4000)
                    else:
                        break

                logger.info("Translation handling complete")
        except Exception as e:
            logger.debug(f"No translation needed or error: {e}")

        # Hide metrics if requested
        if hide_metrics:
            await page.evaluate('''() => {
                document.querySelectorAll('[data-testid="like"], [data-testid="retweet"], [data-testid="reply"]')
                    .forEach(el => {
                        const parent = el.closest('[role="group"]');
                        if (parent) parent.style.display = 'none';
                    });
            }''')

        # Clean up UI
        await page.evaluate('''() => {
            document.querySelector('[data-testid="BottomBar"]')?.remove();
            document.querySelectorAll('[role="dialog"]').forEach(e => e.remove());
            document.querySelector('[data-testid="sidebarColumn"]')?.remove();
            document.querySelector('header[role="banner"]')?.remove();
            document.querySelectorAll('[aria-label="Timeline: Trending now"]').forEach(e => e.remove());
            document.querySelectorAll('[data-testid="sheetDialog"]').forEach(e => e.remove());

            // Hide all articles except the first one (main tweet)
            const articles = document.querySelectorAll('article');
            if (articles.length > 1) {
                for (let i = 1; i < articles.length; i++) {
                    articles[i].style.display = 'none';
                }
            }

            // Remove cellInnerDiv elements AFTER the first article (replies section)
            // But be careful not to remove the article itself
            const cellDivs = document.querySelectorAll('[data-testid="cellInnerDiv"]');
            let foundMainArticle = false;
            cellDivs.forEach(el => {
                const hasArticle = el.querySelector('article');
                if (hasArticle && !foundMainArticle) {
                    foundMainArticle = true;  // This is the main tweet
                } else if (foundMainArticle) {
                    // This is after the main tweet - hide it (replies, "Read X replies", etc)
                    el.style.display = 'none';
                }
            });

            // Hide "Read X replies" link that appears below the metrics bar
            const article = document.querySelector('article');
            if (article) {
                // Find ALL elements and check for "Read X replies" text
                const walker = document.createTreeWalker(article, NodeFilter.SHOW_TEXT, null, false);
                const nodesToHide = [];
                while (walker.nextNode()) {
                    const text = walker.currentNode.textContent || '';
                    if (text.match(/Read\\s+\\d+.*repl/i)) {
                        // Found the text, hide the containing link/div
                        let el = walker.currentNode.parentElement;
                        while (el && el !== article) {
                            // Go up until we find a suitable container to hide
                            if (el.tagName === 'A' || el.getAttribute('role') === 'link' ||
                                (el.tagName === 'DIV' && el.children.length < 5)) {
                                nodesToHide.push(el);
                                break;
                            }
                            el = el.parentElement;
                        }
                    }
                }
                nodesToHide.forEach(el => {
                    // Hide the element and its parent row
                    el.style.display = 'none';
                    if (el.parentElement && el.parentElement !== article) {
                        el.parentElement.style.display = 'none';
                    }
                });

                // Also try direct approach - hide divs containing reply text after action bar
                const groups = article.querySelectorAll('[role="group"]');
                if (groups.length > 0) {
                    const lastGroup = groups[groups.length - 1];  // Action bar (like/retweet)
                    let sibling = lastGroup.parentElement?.nextElementSibling;
                    while (sibling) {
                        sibling.style.display = 'none';
                        sibling = sibling.nextElementSibling;
                    }
                }
            }
        }''')

        await page.wait_for_timeout(500)

        # Wait for quoted tweets / embedded content to load
        try:
            # Multiple selectors for quote tweets (Twitter changes these periodically)
            quote_selectors = [
                '[data-testid="card.wrapper"]',
                '[data-testid="tweetText"] + div > a[href*="/status/"]',  # Quote tweet link
                'article [role="link"][href*="/status/"]',  # Embedded tweet
                '[data-testid="quoteTweet"]',
            ]
            for selector in quote_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=2000)
                    logger.info(f"Found quote element: {selector}")
                    break
                except:
                    continue
            await page.wait_for_timeout(2000)  # Extra time for quote content
        except:
            pass  # No quote tweet

        # Wait for all images including in quoted tweets
        await page.evaluate('''() => {
            return new Promise(resolve => {
                const images = document.querySelectorAll('article img');
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

        # Screenshot the article element - use full_page option for proper capture
        article = await page.query_selector('article')
        if article:
            # Get the bounding box to check size
            box = await article.bounding_box()
            if box:
                logger.info(f"Article size: {box['width']}x{box['height']}")
            screenshot_bytes = await article.screenshot()
            logger.info(f"Captured tweet {parsed['tweet_id']}")
        else:
            screenshot_bytes = await page.screenshot()
            logger.warning("Article not found, captured full page")

        # Save storage state
        await context.storage_state(path=os.path.join(CONFIG_DIR, "storage_state.json"))

        await browser.close()
        await playwright.stop()

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
                await browser.close()
            except:
                pass
        if playwright:
            try:
                await playwright.stop()
            except:
                pass
        return {"success": False, "error": str(e)}


async def perform_auto_login_playwright(page):
    """Auto-login using Playwright"""
    global LAST_LOGIN_ATTEMPT

    creds = load_credentials()
    if not creds:
        logger.error("No credentials stored for auto-login")
        return False

    now = time.time()
    if now - LAST_LOGIN_ATTEMPT < LOGIN_COOLDOWN:
        logger.warning(f"Auto-login cooldown active")
        return False

    LAST_LOGIN_ATTEMPT = now
    logger.info("Starting auto-login with Playwright...")

    try:
        # Go to login page
        await page.goto("https://x.com/login", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)

        # Step 1: Enter username
        logger.info("Step 1: Entering username...")
        try:
            # Try multiple selectors for the username input
            username_input = None
            selectors = [
                'input[autocomplete="username"]',
                'input[name="text"]',
                'input[type="text"]',
                'input'
            ]

            for sel in selectors:
                try:
                    elem = page.locator(sel).first
                    if await elem.count() > 0:
                        username_input = elem
                        logger.info(f"Found input with selector: {sel}")
                        break
                except:
                    continue

            if not username_input:
                logger.error("Could not find any input field")
                return False

            # Click to ensure focus
            await username_input.click()
            await page.wait_for_timeout(1000)

            # Clear any existing value
            await username_input.fill("")
            await page.wait_for_timeout(300)

            # Type username using press_sequentially for reliability
            await username_input.press_sequentially(creds["username"], delay=100)
            await page.wait_for_timeout(500)

            # Verify
            value = await username_input.input_value()
            logger.info(f"Username field value: '{value}'")

            # Debug: screenshot BEFORE clicking Next
            try:
                ss = await page.screenshot()
                with open("/tmp/twitter_screenshots/login_step1_before.png", "wb") as f:
                    f.write(ss)
                logger.info("Saved pre-Next screenshot")
            except:
                pass

        except Exception as e:
            logger.error(f"Could not enter username: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

        await page.wait_for_timeout(1500)

        # Click Next button directly
        logger.info("Clicking Next button...")

        # Find and click the Next button using Playwright locator
        next_btn = page.get_by_role("button", name="Next")
        try:
            await next_btn.click()
            logger.info("Clicked Next button")
        except Exception as e:
            logger.warning(f"Could not click Next button: {e}")
            # Fallback: try clicking by text
            try:
                next_btn2 = page.locator('div[role="button"]:has-text("Next")')
                await next_btn2.first.click()
                logger.info("Clicked Next via role=button")
            except:
                # Last resort: press Enter
                await page.keyboard.press("Enter")
                logger.info("Pressed Enter as fallback")

        # Screenshot immediately after clicking
        await page.wait_for_timeout(1000)
        try:
            ss = await page.screenshot()
            with open("/tmp/twitter_screenshots/login_step1_after.png", "wb") as f:
                f.write(ss)
            logger.info("Saved post-Next screenshot")
        except:
            pass

        await page.wait_for_timeout(5000)

        # Debug: Save screenshot of login state
        try:
            debug_ss = await page.screenshot()
            with open("/tmp/twitter_screenshots/login_debug.png", "wb") as f:
                f.write(debug_ss)
            logger.info("Saved login debug screenshot")
        except:
            pass

        # Step 2: Check for verification (phone/email/username)
        page_text = await page.evaluate('document.body.innerText')
        logger.info(f"After Step 1: {page_text[:150]}")

        # Handle verification challenge
        if "phone" in page_text.lower() or "email" in page_text.lower() or "username" in page_text.lower():
            if "password" not in page_text.lower():
                logger.info("Step 2: Verification challenge detected...")
                verify_input = page.locator('input[name="text"], input[data-testid="ocfEnterTextTextInput"]')
                try:
                    await verify_input.first.wait_for(timeout=5000)
                    await verify_input.first.fill(creds["username"])
                    await page.wait_for_timeout(500)

                    next_btn = page.locator('button:has-text("Next"), div[role="button"]:has-text("Next")')
                    await next_btn.first.click()
                    await page.wait_for_timeout(5000)
                    logger.info("Verification completed")
                except Exception as e:
                    logger.warning(f"Verification failed: {e}")

        # Step 3: Enter password
        logger.info("Step 3: Entering password...")
        password_input = page.locator('input[name="password"], input[type="password"]')
        try:
            await password_input.first.wait_for(timeout=10000)
        except:
            # Take screenshot for debugging
            page_text = await page.evaluate('document.body.innerText')
            logger.error(f"Password field not found. Page: {page_text[:300]}")
            return False

        await password_input.first.fill(creds["password"])
        logger.info("Password entered")
        await page.wait_for_timeout(1000)

        # Click Log in button
        logger.info("Clicking Log in...")
        login_btn = page.locator('button:has-text("Log in"), div[role="button"]:has-text("Log in")')
        try:
            await login_btn.first.click()
            logger.info("Clicked Log in")
        except:
            await page.keyboard.press("Enter")
            logger.info("Pressed Enter")

        await page.wait_for_timeout(8000)

        # Check success
        current_url = page.url
        logger.info(f"Final URL: {current_url}")

        if "home" in current_url.lower():
            logger.info("Auto-login successful!")
            return True

        page_text = await page.evaluate('document.body.innerText')
        if "Wrong password" in page_text or "incorrect" in page_text.lower():
            logger.error("Auto-login failed: incorrect password")
            return False

        if "Something went wrong" in page_text:
            logger.error("Auto-login failed: Twitter error")
            return False

        # Check if we can see compose button (logged in indicator)
        compose = await page.query_selector('[data-testid="SideNav_NewTweet_Button"]')
        if compose:
            logger.info("Auto-login successful (found compose button)")
            return True

        logger.info(f"Login status unclear, URL: {current_url}")
        return True

    except Exception as e:
        logger.error(f"Auto-login error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def capture_tweet_sync(url, parsed, theme="dark", hide_metrics=False, width=550):
    """Synchronous wrapper for async capture"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(
            capture_tweet_playwright(url, parsed, theme, hide_metrics, width)
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


@app.route("/api/credentials", methods=["GET"])
def credentials_status():
    """Check if credentials are configured"""
    creds = load_credentials()
    if creds:
        return jsonify({
            "configured": True,
            "username": creds.get("username", "")[:3] + "***"  # Masked
        })
    return jsonify({"configured": False})


@app.route("/api/credentials", methods=["POST"])
def credentials_save():
    """Save Twitter credentials for auto-login"""
    data = request.json
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400

    username = data.get("username", "").strip()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify({"success": False, "error": "Username and password required"}), 400

    if save_credentials(username, password):
        return jsonify({
            "success": True,
            "message": "Credentials saved. Auto-login will be attempted when needed."
        })
    return jsonify({"success": False, "error": "Failed to save credentials"}), 500


@app.route("/api/credentials", methods=["DELETE"])
def credentials_clear():
    """Clear stored credentials"""
    if os.path.exists(CREDENTIALS_FILE):
        os.remove(CREDENTIALS_FILE)
        return jsonify({"success": True, "message": "Credentials cleared"})
    return jsonify({"success": True, "message": "No credentials to clear"})


@app.route("/api/capture", methods=["POST"])
def capture():
    data = request.json
    url = data.get("url", "").strip()
    theme = data.get("theme", "dark")
    hide_metrics = data.get("hide_metrics", False)
    width = max(300, min(800, int(data.get("width", 550))))

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
    width = max(300, min(800, int(data.get('width', 550))))

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
