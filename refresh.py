#!/usr/bin/env python3
"""
Twitter auth refresh — runs INSIDE the twitter-screenshot container.

Opens /opt/twitter-screenshot/config/storage_state.json in headless Playwright
and visits x.com/home so Twitter rotates short-lived cookies (ct0, __cf_bm,
csrf-related). Saves refreshed storage_state back.

If the warm visit lands at /login, falls back to a full username/password
login using credentials.json (same flow the app uses).

Logs to /opt/twitter-screenshot/config/refresh.log (truncates at 200KB).

Schedule via cron:
    docker exec twitter-screenshot python /app/refresh.py
"""

import asyncio
import datetime
import json
import os
import shutil
import sys
import traceback

from playwright.async_api import async_playwright

CONFIG_DIR = "/opt/twitter-screenshot/config"
# TWITTER_STORAGE_OVERRIDE lets sandbox tests target a copy instead of the live
# file — used to verify fresh-login flow without risking the working session.
STORAGE_STATE = os.environ.get(
    "TWITTER_STORAGE_OVERRIDE",
    os.path.join(CONFIG_DIR, "storage_state.json"),
)
CREDENTIALS_FILE = os.path.join(CONFIG_DIR, "credentials.json")
LOG_FILE = os.path.join(CONFIG_DIR, "refresh.log")
LOG_MAX = 200_000

HOME_URL = "https://x.com/home"
LOGIN_URL = "https://x.com/login"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX:
            os.remove(LOG_FILE)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_creds():
    if not os.path.exists(CREDENTIALS_FILE):
        return None
    try:
        with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if d.get("username") and d.get("password"):
            return d
    except Exception as e:
        log(f"creds load err: {e}")
    return None


async def save_storage_with_backup(context, target_path: str) -> None:
    """Back up the current good state, then atomically replace with fresh state.
    If the new save fails, the .bak survives so live state is recoverable."""
    if os.path.exists(target_path):
        try:
            shutil.copy2(target_path, target_path + ".bak")
        except Exception as e:
            log(f"backup failed (continuing anyway): {e}")
    tmp = target_path + ".tmp"
    await context.storage_state(path=tmp)
    # Sanity check: must have cookies and at least one Twitter auth cookie
    try:
        with open(tmp, "r", encoding="utf-8") as f:
            d = json.load(f)
        cookies = d.get("cookies", [])
        has_auth = any(
            c.get("name") in ("auth_token", "ct0") and c.get("value")
            for c in cookies
        )
        if not has_auth:
            raise ValueError("no auth_token/ct0 in saved state")
    except Exception as e:
        os.remove(tmp)
        raise RuntimeError(f"refused to overwrite — fresh state invalid: {e}")
    os.replace(tmp, target_path)


async def is_logged_in(page) -> bool:
    url = page.url
    if "/login" in url or "/i/flow/login" in url or "/account/access" in url:
        return False
    # Real "logged in" check: compose button present in side nav on /home
    try:
        await page.wait_for_selector(
            "[data-testid='SideNav_NewTweet_Button'], "
            "[data-testid='AppTabBar_Home_Link'], "
            "a[href='/home']",
            timeout=8000,
        )
        return True
    except Exception:
        return False


async def warm_session() -> bool:
    if not os.path.exists(STORAGE_STATE):
        log(f"{STORAGE_STATE} missing — fresh login required")
        return False
    log("Warming session via stored storage_state")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--lang=en-US",
                "--accept-lang=en-US,en",
            ],
        )
        try:
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                device_scale_factor=1,
                user_agent=UA,
                locale="en-US",
                timezone_id="America/New_York",
                storage_state=STORAGE_STATE,
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                "window.chrome = {runtime: {}};"
            )
            page = await context.new_page()
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(6000)

            if not await is_logged_in(page):
                log(f"Bounced or stale (url={page.url}) — session is dead")
                await context.close()
                return False

            await save_storage_with_backup(context, STORAGE_STATE)
            log(f"Refreshed {STORAGE_STATE} (url={page.url})")
            await context.close()
            return True
        finally:
            await browser.close()


async def fresh_login() -> bool:
    creds = load_creds()
    if not creds:
        log(f"{CREDENTIALS_FILE} missing or incomplete — cannot do fresh login")
        return False
    user = creds["username"]
    pw = creds["password"]
    log(f"Performing fresh login for {user}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--lang=en-US",
                "--accept-lang=en-US,en",
            ],
        )
        try:
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=UA,
                locale="en-US",
                timezone_id="America/New_York",
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                "window.chrome = {runtime: {}};"
            )
            page = await context.new_page()
            await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(5000)

            # Step 1 — username
            uname_input = None
            for sel in (
                'input[autocomplete="username"]',
                'input[name="text"]',
                'input[type="text"]',
                "input",
            ):
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    uname_input = loc
                    break
            if not uname_input:
                log("Could not find username input")
                return False
            await uname_input.click()
            await page.wait_for_timeout(800)
            await uname_input.fill("")
            await uname_input.press_sequentially(user, delay=80)
            await page.wait_for_timeout(800)

            # Click Next
            try:
                await page.get_by_role("button", name="Next").click(timeout=5000)
            except Exception:
                try:
                    await page.locator('div[role="button"]:has-text("Next")').first.click()
                except Exception:
                    await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)

            # Possible verification challenge
            page_text = await page.evaluate("document.body.innerText")
            lower = page_text.lower()
            if ("phone" in lower or "email" in lower or "username" in lower) and "password" not in lower:
                log("Verification challenge detected — re-supplying username")
                try:
                    verify = page.locator(
                        "input[name='text'], input[data-testid='ocfEnterTextTextInput']"
                    ).first
                    await verify.wait_for(timeout=5000)
                    await verify.fill(user)
                    await page.wait_for_timeout(500)
                    try:
                        await page.locator(
                            "button:has-text('Next'), div[role='button']:has-text('Next')"
                        ).first.click()
                    except Exception:
                        await page.keyboard.press("Enter")
                    await page.wait_for_timeout(5000)
                except Exception as e:
                    log(f"Verification step failed: {e}")

            # Step 2 — password
            pw_input = page.locator('input[name="password"], input[type="password"]').first
            try:
                await pw_input.wait_for(timeout=10000)
            except Exception:
                text = await page.evaluate("document.body.innerText")
                log(f"Password field missing. Page text preview: {text[:200]}")
                return False
            await pw_input.fill(pw)
            await page.wait_for_timeout(800)
            try:
                await page.locator(
                    "button:has-text('Log in'), div[role='button']:has-text('Log in')"
                ).first.click()
            except Exception:
                await page.keyboard.press("Enter")
            await page.wait_for_timeout(9000)

            # Verify
            if not await is_logged_in(page):
                text = await page.evaluate("document.body.innerText")
                log(f"Login did not land at home (url={page.url}). Text: {text[:200]}")
                return False

            await save_storage_with_backup(context, STORAGE_STATE)
            log(f"Fresh login OK; saved {STORAGE_STATE}")
            await context.close()
            return True
        finally:
            await browser.close()


async def main_async() -> int:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    if await warm_session():
        return 0
    log("warm_session failed — attempting fresh login")
    if await fresh_login():
        return 0
    log("FAIL: both warm + fresh login failed")
    return 1


def main() -> int:
    try:
        return asyncio.run(main_async())
    except Exception as e:
        log(f"FAIL: {e}")
        log(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
