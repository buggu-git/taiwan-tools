#!/bin/bash
# Setup cron job for daily ETF scraping

PROJECT_DIR="/Users/buggu/clawd/projects/taiwan_etf_holdings"
PYTHON_PATH="/opt/homebrew/bin/python3"
CRON_ID="taiwan_etf_scrape"

echo "=== Setting up cron job for ETF scraper ==="

# Remove existing cron job
crontab -l | grep -v "$CRON_ID" | crontab - 2>/dev/null || true

# Add new cron job (daily at 8:00 AM Taipei time)
CRON_LINE="0 8 * * * cd $PROJECT_DIR && $PYTHON_PATH scheduler.py --once >> $PROJECT_DIR/logs/cron.log 2>&1"

(crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -

echo "Cron job added:"
crontab -l | grep "$CRON_ID"

echo ""
echo "=== Setup complete ==="
echo "Scraper will run daily at 8:00 AM Taipei time"
