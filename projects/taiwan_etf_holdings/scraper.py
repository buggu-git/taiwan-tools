#!/usr/bin/env python3
"""
Taiwan ETF Holdings Scraper
Uses Zyte API for JavaScript rendering
"""

import asyncio
import aiohttp
import json
import yaml
import base64
import logging
from datetime import datetime, date
from typing import List, Dict, Optional
from dataclasses import dataclass
import psycopg2
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ETFConfig:
    symbol: str
    name: str
    provider: str
    type: str
    url: str

@dataclass
class Holding:
    etf_symbol: str
    trade_date: date
    holding_date: date
    rank: int
    isin: Optional[str]
    issuer_name: str
    security_name: str
    security_type: str
    shares_held: float
    market_value_twd: float
    weight_pct: float
    source_url: str

class ZyteClient:
    """Zyte API client for JavaScript rendering"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('ZYTE_API_KEY', '')
        self.api_url = 'https://api.zyte.com/v1/extract'
        
    async def fetch(self, url: str) -> Optional[str]:
        """Fetch page via Zyte API - returns HTML or None if fails"""
        if not self.api_key:
            logger.warning("No Zyte API key configured")
            return None
            
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "url": url,
                    "httpResponseBody": True,
                }
                
                # Zyte uses HTTP Basic Auth with api_key as username and empty password
                async with session.post(
                    self.api_url,
                    json=payload,
                    auth=aiohttp.BasicAuth(self.api_key, '')
                ) as resp:
                    
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"Zyte API error {resp.status}: {error_text[:200]}")
                        return None
                    
                    data = await resp.json()
                    
                    # Decode base64 HTML response
                    html_b64 = data.get("httpResponseBody")
                    if html_b64:
                        html = base64.b64decode(html_b64).decode('utf-8', errors='ignore')
                        logger.info(f"Successfully fetched {len(html)} chars from Zyte")
                        return html
                    
                    logger.warning("No httpResponseBody in Zyte response")
                    return None
                    
        except Exception as e:
            logger.error(f"Zyte fetch error: {e}")
        return None

class DirectClient:
    """Direct HTTP client"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
    async def fetch(self, url: str) -> Optional[str]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    logger.warning(f"HTTP {resp.status} for {url}")
        except Exception as e:
            logger.error(f"HTTP fetch error: {e}")
        return None

class ETFScraper:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.etfs = self._parse_etfs()
        self._init_db()
        
        zyte_key = os.getenv('ZYTE_API_KEY', '')
        self.zyte = ZyteClient(zyte_key)
        self.direct = DirectClient()
        
    def _load_config(self) -> dict:
        with open(self.config_path) as f:
            return yaml.safe_load(f)
    
    def _parse_etfs(self) -> List[ETFConfig]:
        return [ETFConfig(**e) for e in self.config['etfs']]
    
    def _get_db_connection(self):
        db_config = self.config['database']
        return psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['name'],
            user=db_config['user'],
            password=os.getenv('DB_PASSWORD', db_config.get('password', ''))
        )
    
    def _init_db(self):
        conn = self._get_db_connection()
        with conn.cursor() as cursor:
            with open("schema.sql") as f:
                cursor.execute(f.read())
            conn.commit()
            
            for etf in self.etfs:
                cursor.execute('''
                    INSERT INTO etf_master (symbol, name, provider, type, zyte_url)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE 
                    SET name = EXCLUDED.name, updated_at = NOW()
                ''', (etf.symbol, etf.name, etf.provider, etf.type, etf.url))
            conn.commit()
        conn.close()
        logger.info(f"DB ready with {len(self.etfs)} ETFs")
    
    def parse_holdings(self, html: str, etf_symbol: str) -> List[Holding]:
        holdings = []
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all(['td', 'th'])
                if len(cols) >= 4:
                    try:
                        holding = Holding(
                            etf_symbol=etf_symbol,
                            trade_date=date.today(),
                            holding_date=date.today(),
                            rank=len(holdings) + 1,
                            isin=None,
                            issuer_name=cols[0].get_text(strip=True),
                            security_name=cols[1].get_text(strip=True) if len(cols) > 1 else '',
                            security_type='',
                            shares_held=0,
                            market_value_twd=0,
                            weight_pct=float(cols[-1].get_text(strip=True).replace('%', '').replace(',', '')) if cols[-1].get_text(strip=True) else 0,
                            source_url=''
                        )
                        holdings.append(holding)
                    except (ValueError, IndexError):
                        continue
        return holdings
    
    async def fetch_etf(self, etf: ETFConfig) -> List[Holding]:
        logger.info(f"Fetching {etf.symbol} - {etf.name}")
        html = await self.zyte.fetch(etf.url)
        if html:
            return self.parse_holdings(html, etf.symbol)
        return []
    
    async def fetch_all(self) -> Dict[str, List[Holding]]:
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_etf(etf) for etf in self.etfs]
            results = await asyncio.gather(*tasks)
            return dict(zip([e.symbol for e in self.etfs], results))
    
    def save_holdings(self, holdings_dict: Dict[str, List[Holding]]):
        conn = self._get_db_connection()
        with conn.cursor() as cursor:
            total = 0
            for symbol, holdings in holdings_dict.items():
                for h in holdings:
                    cursor.execute('''
                        INSERT INTO etf_holdings 
                        (etf_symbol, trade_date, holding_date, rank, isin, 
                         issuer_name, security_name, security_type,
                         shares_held, market_value_twd, weight_pct, source_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        h.etf_symbol, h.trade_date, h.holding_date, h.rank, h.isin,
                        h.issuer_name, h.security_name, h.security_type,
                        h.shares_held, h.market_value_twd, h.weight_pct, h.source_url
                    ))
                    total += 1
            conn.commit()
        conn.close()
        logger.info(f"Saved {total} holdings for {len(holdings_dict)} ETFs")
    
    async def run(self):
        logger.info("Starting ETF holdings fetch...")
        holdings = await self.fetch_all()
        self.save_holdings(holdings)
        
        for symbol, h in holdings.items():
            logger.info(f"{symbol}: {len(h)} holdings")

if __name__ == "__main__":
    scraper = ETFScraper()
    asyncio.run(scraper.run())
