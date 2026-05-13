FROM mcr.microsoft.com/playwright/python:v1.52.0-noble

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install playwright
RUN playwright install chromium

COPY . .

# Create directories
RUN mkdir -p /opt/twitter-screenshot/config/chrome_profile

EXPOSE 8891

ENV DISPLAY=:99

CMD Xvfb :99 -screen 0 1920x1080x24 & sleep 2 && python app.py
