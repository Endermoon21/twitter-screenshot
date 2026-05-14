#!/usr/bin/env python3
"""
Twitter Remote Login Helper
- Starts Chrome with remote debugging
- You connect and log in manually
- Extracts cookies when done
"""
import subprocess
import time
import json
import os
import requests
import websocket

CHROME_PATH = '/root/.cache/ms-playwright/chromium-1200/chrome-linux64/chrome'
DEBUG_PORT = 9222
CONFIG_DIR = '/opt/twitter-screenshot/config'

def start_chrome():
    print('Starting Chrome with remote debugging...')
    proc = subprocess.Popen([
        CHROME_PATH,
        f'--remote-debugging-port={DEBUG_PORT}',
        '--remote-debugging-address=0.0.0.0',
        '--remote-allow-origins=*',
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--window-size=1280,900',
        'https://x.com/login'
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ, 'DISPLAY': ':99'})
    return proc

def get_cookies():
    try:
        # Get WebSocket URL
        res = requests.get(f'http://localhost:{DEBUG_PORT}/json', timeout=5)
        targets = res.json()
        ws_url = None
        for t in targets:
            if 'webSocketDebuggerUrl' in t:
                ws_url = t['webSocketDebuggerUrl']
                break
        
        if not ws_url:
            return None
            
        # Connect and get cookies
        ws = websocket.create_connection(ws_url)
        ws.send(json.dumps({'id': 1, 'method': 'Network.getAllCookies'}))
        response = json.loads(ws.recv())
        ws.close()
        
        return response.get('result', {}).get('cookies', [])
    except Exception as e:
        print(f'Error getting cookies: {e}')
        return None

def main():
    # Kill any existing Chrome
    subprocess.run(['pkill', '-f', 'chrome'], capture_output=True)
    time.sleep(1)
    
    # Start Chrome
    proc = start_chrome()
    time.sleep(3)
    
    print()
    print('=' * 60)
    print('Chrome is running with remote debugging!')
    print('=' * 60)
    print()
    print('Connect from your browser:')
    print(f'  chrome://inspect/#devices')
    print(f'  Then click "Configure" and add: 100.89.14.34:{DEBUG_PORT}')
    print()
    print('Or open DevTools directly:')
    print(f'  http://100.89.14.34:{DEBUG_PORT}')
    print()
    print('Log into Twitter, then press ENTER here to extract cookies...')
    print()
    
    input('Press ENTER after logging in: ')
    
    print('Extracting cookies...')
    cookies = get_cookies()
    
    if cookies:
        # Filter for Twitter cookies
        twitter_cookies = [c for c in cookies if '.x.com' in c.get('domain', '') or 'twitter.com' in c.get('domain', '')]
        auth_cookies = [c for c in twitter_cookies if c['name'] in ('auth_token', 'ct0', 'twid')]
        
        print(f'Found {len(twitter_cookies)} Twitter cookies')
        print(f'Auth cookies: {[c["name"] for c in auth_cookies]}')
        
        if auth_cookies:
            # Save
            os.makedirs(CONFIG_DIR, exist_ok=True)
            cookie_list = [{'name': c['name'], 'value': c['value'], 'domain': c['domain'], 
                           'path': c.get('path', '/'), 'secure': c.get('secure', True)} 
                          for c in twitter_cookies]
            
            with open(f'{CONFIG_DIR}/auth.json', 'w') as f:
                json.dump({'cookies': cookie_list, 'timestamp': __import__('datetime').datetime.now().isoformat()}, f, indent=2)
            
            print()
            print('SUCCESS! Cookies saved.')
            print(f'auth_token: {next((c["value"][:20] + "..." for c in auth_cookies if c["name"] == "auth_token"), "not found")}')
        else:
            print('ERROR: No auth cookies found. Make sure you logged in successfully.')
    else:
        print('ERROR: Could not extract cookies')
    
    # Cleanup
    proc.terminate()
    print('Chrome stopped.')

if __name__ == '__main__':
    main()
