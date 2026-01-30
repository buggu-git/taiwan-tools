#!/usr/bin/env python3
"""
Daily Scheduler for Taiwan ETF Holdings Scraper
Uses APScheduler for robust scheduling with retry logic
"""

import asyncio
import yaml
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import pytz

from scraper import PostgresETFScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ETFScheduler:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Taipei'))
        self.scraper = None
        
    def _load_config(self) -> dict:
        with open(self.config_path) as f:
            return yaml.safe_load(f)
    
    def job_listener(self, event):
        """Listen for job events"""
        if event.exception:
            logger.error(f"Job {event.job_id} failed: {event.exception}")
        else:
            logger.info(f"Job {event.job_id} executed successfully")
    
    async def run_scraper(self):
        """Execute the scraper job"""
        logger.info("Starting scheduled scrape...")
        try:
            self.scraper = PostgresETFScraper(self.config_path)
            await self.scraper.run()
            logger.info("Scheduled scrape completed")
        except Exception as e:
            logger.error(f"Scheduled scrape failed: {e}")
            raise
    
    def start(self):
        """Start the scheduler"""
        schedule_config = self.config.get('schedule', {})
        cron_time = schedule_config.get('daily_fetch', '08:00 Asia/Taipei')
        
        # Parse time (assume 8:00 AM if only time given)
        hour, minute = 8, 0
        if ':' in cron_time:
            time_part = cron_time.split()[0] if ' ' in cron_time else cron_time
            h, m = time_part.split(':')
            hour, minute = int(h), int(m)
        
        # Add job
        self.scheduler.add_job(
            self.run_scraper,
            CronTrigger(hour=hour, minute=minute, timezone='Asia/Taipei'),
            id='etf_daily_scrape',
            name='Daily ETF Holdings Scrape',
            replace_existing=True,
            max_instances=1
        )
        
        # Add event listener
        self.scheduler.add_listener(
            self.job_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        )
        
        self.scheduler.start()
        logger.info(f"Scheduler started. Next scrape at {hour:02d}:{minute:02d} Asia/Taipei")
        
        return self.scheduler
    
    def run_once(self):
        """Run scraper once immediately"""
        asyncio.run(self.run_scraper())

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        # Run once and exit
        scheduler = ETFScheduler()
        scheduler.run_once()
    else:
        # Start scheduler
        scheduler = ETFScheduler()
        scheduler.start()
        
        # Keep running
        try:
            import asyncio
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            pass
