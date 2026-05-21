#!/usr/bin/env python3
"""
Twitter session keepalive.

Runs every 6 hours via systemd timer. Each run:
  1. Loads current cookies from /opt/twitter-screenshot/config/auth.json
  2. Launches headless Chromium with those cookies
  3. Navigates to https://x.com/home (logged-in only page)
  4. Verifies the session is still authenticated by checking page title
     (Logged-out users get title "X"; logged-in users see notification counts
     like "(3) X" or actual content)
  5. If logged in: extracts the current (possibly rotated) cookies and
     atomically rewrites auth.json — keeps the session warm
  6. If logged out: leaves auth.json untouched and fires HA notification

This mirrors the pattern Tumblr's keepalive.py uses successfully.

Exit codes:
  0 = session healthy, cookies refreshed
  1 = session dead, HA notified
  2 = setup error (missing file, etc.)
"""

import json
import os
import sys
import logging
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: playwright not installed in this venv", file=sys.stderr)
    sys.exit(2)

# === Config ===
AUTH_FILE = Path("/opt/twitter-screenshot/config/auth.json")
LOG_FILE = Path("/var/log/twitter-keepalive.log")
HA_TOKEN_FILE = Path("/opt/screenshot-sync/.ha-token")
HA_URL = "http://localhost:8123/api/services/persistent_notification/create"

ESSENTIAL_COOKIES = {"auth_token", "ct0", "twid", "kdt", "guest_id", "personalization_id"}
KEEPALIVE_URL = "https://x.com/home"

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("twitter-keepalive")


def notify_ha(title: str, message: str) -> None:
    """Fire a persistent_notification in Home Assistant. Best-effort."""
    if not HA_TOKEN_FILE.exists():
        log.warning("HA token file missing, skipping notification")
        return
    token = HA_TOKEN_FILE.read_text().strip()
    payload = json.dumps({
        "title": title,
        "message": message,
        "notification_id": "twitter-auth-fail",
    })
    try:
        subprocess.run(
            ["curl", "-s", "-m", "10", "-X", "POST", HA_URL,
             "-H", f"Authorization: Bearer {token}",
             "-H", "Content-Type: application/json",
             "--data", payload],
            check=True, timeout=15,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        log.info(f"HA notified: {title}")
    except Exception as e:
        log.warning(f"HA notification failed: {e}")


def load_cookies() -> list[dict]:
    """Read auth.json and convert to Playwright cookie format."""
    if not AUTH_FILE.exists():
        log.error(f"auth file missing: {AUTH_FILE}")
        return []
    data = json.loads(AUTH_FILE.read_text())
    cookies = []
    for c in data.get("cookies", []):
        pw_cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ".x.com"),
            "path": c.get("path", "/"),
            "secure": c.get("secure", True),
            "httpOnly": c.get("httpOnly", False),
            "sameSite": c.get("sameSite", "Lax"),
        }
        if c.get("expires") or c.get("expirationDate"):
            pw_cookie["expires"] = c.get("expires") or c.get("expirationDate")
        cookies.append(pw_cookie)
    return cookies


def atomic_write_auth(cookies: list[dict]) -> None:
    """Write auth.json atomically (write to .tmp, then rename)."""
    payload = {
        "cookies": cookies,
        "timestamp": datetime.now().isoformat(),
    }
    fd, tmp_path = tempfile.mkstemp(
        prefix="auth.", suffix=".tmp", dir=str(AUTH_FILE.parent)
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, AUTH_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def is_logged_out(title: str) -> bool:
    """Detect a logged-out page based on its title."""
    # Logged-in pages have notification counts like "(3) Home / X" or
    # "(3) X. It's what's happening / X". Login wall pages typically have
    # just "X" or "X. It's what's happening / X" without a count prefix
    # AND we navigated to /home, so if Twitter served the login wall, the
    # title won't include "Home". Anchor on that.
    title_lower = title.lower()
    if "log in" in title_lower or "sign up" in title_lower:
        return True
    if "/i/flow/login" in title_lower:
        return True
    # If we navigated to /home but title doesn't say "home", we got redirected
    # to a login flow.
    if "home" not in title_lower and "what" not in title_lower:
        # "What's happening" is the default landing page title (also logged-in)
        return True
    return False


def main() -> int:
    log.info("=" * 60)
    log.info("Twitter keepalive starting")

    cookies = load_cookies()
    if not cookies:
        log.error("no cookies to apply, bailing")
        notify_ha("Twitter keepalive FAILED",
                  "auth.json missing or empty — manual relogin needed")
        return 2

    auth_token_count = sum(1 for c in cookies if c["name"] == "auth_token")
    log.info(f"loaded {len(cookies)} cookies (auth_token: {auth_token_count})")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,  # No display needed; keepalive doesn't screenshot
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            storage_state={"cookies": cookies, "origins": []},
        )
        page = context.new_page()

        try:
            log.info(f"navigating to {KEEPALIVE_URL}")
            page.goto(KEEPALIVE_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(4000)

            final_url = page.url
            title = page.title()
            log.info(f"final url: {final_url}")
            log.info(f"page title: {title[:120]}")

            if is_logged_out(title):
                log.error("session appears logged out")
                notify_ha(
                    "Twitter session DEAD",
                    f"Keepalive detected logged-out state at {datetime.now().strftime('%H:%M')}. "
                    f"Page title: {title[:80]}. Manual relogin needed — "
                    f"cookies in /opt/twitter-screenshot/config/auth.json are stale.",
                )
                return 1

            # Session healthy — pull the fresh (possibly rotated) cookies
            new_cookies_raw = context.cookies()
            new_cookies = []
            for c in new_cookies_raw:
                domain = c.get("domain", "")
                if not (domain.endswith("x.com") or domain.endswith("twitter.com")):
                    continue
                if c["name"] not in ESSENTIAL_COOKIES:
                    continue
                new_cookies.append({
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c["domain"],
                    "path": c.get("path", "/"),
                    "secure": c.get("secure", True),
                    "httpOnly": c.get("httpOnly", False),
                    "sameSite": c.get("sameSite", "Lax"),
                    "expires": c.get("expires", -1),
                })

            essential_present = {c["name"] for c in new_cookies}
            missing = ESSENTIAL_COOKIES - essential_present
            if "auth_token" in missing:
                log.error("auth_token missing after navigation — refusing to save")
                notify_ha(
                    "Twitter keepalive WARNING",
                    "Page loaded but auth_token cookie not returned — possible "
                    "partial session loss. auth.json NOT updated.",
                )
                return 1
            if missing:
                log.warning(f"some cookies missing after refresh: {missing}")

            atomic_write_auth(new_cookies)
            log.info(f"auth.json refreshed with {len(new_cookies)} cookies")
            return 0

        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log.exception(f"keepalive crashed: {e}")
        notify_ha("Twitter keepalive CRASHED", f"Unhandled exception: {e}")
        sys.exit(2)
