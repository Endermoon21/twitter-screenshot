#!/usr/bin/env python3
from flask import Flask, render_template, request, jsonify, send_file, make_response
import os
import asyncio
import re
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

app = Flask(__name__, static_folder="static", template_folder="static")

SAVE_DIR = "/tmp/twitter_screenshots"
os.makedirs(SAVE_DIR, exist_ok=True)

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

async def capture_tweet_async(url, parsed, theme="dark", hide_metrics=False, width=600):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": width, "height": 1200},
            color_scheme=theme,
            device_scale_factor=2,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            tweet_selector = "article[data-testid=\"tweet\"]"
            try:
                await page.wait_for_selector(tweet_selector, timeout=15000)
            except:
                tweet_selector = "[data-testid=\"tweetText\"]"
                await page.wait_for_selector(tweet_selector, timeout=5000)
            await page.wait_for_timeout(2000)

            # Expand Show more links
            show_more_selectors = [
                "[data-testid=\"tweet-text-show-more-link\"]",
                "article [role=\"link\"]:has-text(\"Show more\")",
            ]
            for selector in show_more_selectors:
                try:
                    buttons = await page.query_selector_all(selector)
                    for btn in buttons:
                        try:
                            await btn.click()
                            await page.wait_for_timeout(500)
                        except:
                            pass
                except:
                    pass
            await page.wait_for_timeout(500)

            if hide_metrics:
                await page.evaluate("""() => {
                    const metrics = document.querySelectorAll("[data-testid=\"like\"], [data-testid=\"retweet\"]");
                    metrics.forEach(el => {
                        const parent = el.closest("[role=\"group\"]");
                        if (parent) parent.style.display = "none";
                    });
                }""")

            # Force tweet width via CSS injection
            await page.evaluate("""(targetWidth) => {
                const tweet = document.querySelector('article[data-testid="tweet"]');
                if (tweet) {
                    // Force width on the tweet article
                    tweet.style.width = targetWidth + 'px';
                    tweet.style.maxWidth = targetWidth + 'px';
                    tweet.style.minWidth = targetWidth + 'px';

                    // Also force width on parent containers that might constrain it
                    let parent = tweet.parentElement;
                    for (let i = 0; i < 5 && parent; i++) {
                        parent.style.width = targetWidth + 'px';
                        parent.style.maxWidth = targetWidth + 'px';
                        parent = parent.parentElement;
                    }
                }
            }""", width)
            await page.wait_for_timeout(300)

            tweet_element = await page.query_selector("article[data-testid=\"tweet\"]")
            if not tweet_element:
                await browser.close()
                return {"success": False, "error": "Could not find tweet"}

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tweet_{parsed['username']}_{parsed['tweet_id']}_{timestamp}.png"
            filepath = os.path.join(SAVE_DIR, filename)
            await tweet_element.screenshot(path=filepath)
            await browser.close()
            return {"success": True, "filepath": filepath, "filename": filename}
        except Exception as e:
            await browser.close()
            return {"success": False, "error": str(e)}

@app.route("/")
def index():
    response = make_response(render_template("web.html"))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route("/api/capture", methods=["POST"])
def capture():
    data = request.json
    url = data.get("url", "").strip()
    theme = data.get("theme", "dark")
    hide_metrics = data.get("hide_metrics", False)
    width = max(400, min(800, int(data.get("width", 600))))
    if not url:
        return jsonify({"success": False, "error": "No URL"})
    parsed = parse_tweet_url(url)
    if not parsed:
        return jsonify({"success": False, "error": "Invalid URL"})
    result = asyncio.run(capture_tweet_async(url, parsed, theme, hide_metrics, width))
    return jsonify(result)

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8891, debug=False)
