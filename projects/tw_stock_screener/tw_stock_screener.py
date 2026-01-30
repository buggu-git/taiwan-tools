#!/usr/bin/env python3
"""
Taiwan Stock Market Top Performers Scraper
Gets yesterday's top gaining stocks from Yahoo Finance
"""

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import subprocess

def get_taiwan_stocks():
    """Scrape Taiwan stocks from Yahoo Finance"""
    
    # Yahoo Finance Taiwan market movers - top gainers
    url = "https://finance.yahoo.com/markets/tw/stocks/gainers/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find the table with stock data
        stocks = []
        table = soup.find('table')
        
        if table:
            rows = table.find_all('tr')[1:11]  # Top 10 gainers
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 6:
                    symbol = cols[0].get_text(strip=True)
                    name = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                    price = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                    change = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                    pct_change = cols[4].get_text(strip=True) if len(cols) > 4 else ""
                    volume = cols[5].get_text(strip=True) if len(cols) > 5 else ""
                    
                    stocks.append({
                        'symbol': symbol,
                        'name': name,
                        'price': price,
                        'change': change,
                        'pct_change': pct_change,
                        'volume': volume
                    })
        
        return stocks
        
    except Exception as e:
        print(f"Error scraping Yahoo Finance: {e}")
        return None

def get_market_summary():
    """Alternative: Get market summary from TradingView"""
    
    url = "https://www.tradingview.com/markets/stocks/taiwan/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Look for top gainers section
        gainers = []
        
        # Try to find gainers table
        tables = soup.find_all('table')
        for table in tables[:3]:
            rows = table.find_all('tr')[1:6]
            for row in rows:
                cols = row.find_all(['td', 'th'])
                if len(cols) >= 3:
                    symbol_elem = cols[0].find('a')
                    symbol = symbol_elem.get_text(strip=True) if symbol_elem else cols[0].get_text(strip=True)
                    change_elem = cols[-1]  # Last column usually % change
                    pct = change_elem.get_text(strip=True)
                    
                    if symbol and '%' in pct:
                        gainers.append({'symbol': symbol, 'pct': pct})
        
        return gainers[:5]
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def main():
    print("=" * 60)
    print("📈 Taiwan Stock Market - Top Performers")
    print("=" * 60)
    
    yesterday = datetime.now() - timedelta(days=1)
    print(f"📅 Data for: {yesterday.strftime('%Y-%m-%d')}\n")
    
    # Try Yahoo Finance
    print("🔍 Fetching from Yahoo Finance...")
    stocks = get_taiwan_stocks()
    
    if stocks and len(stocks) > 0:
        print(f"\n🏆 TOP 5 GAINERS:\n")
        
        for i, stock in enumerate(stocks[:5], 1):
            print(f"  {i}. {stock['symbol']} ({stock['name']})")
            print(f"     💰 Price: {stock['price']}")
            print(f"     📈 Change: {stock['change']} ({stock['pct_change']})")
            print(f"     📊 Volume: {stock['volume']}")
            print()
        
        # Top performer
        top = stocks[0]
        print("=" * 60)
        print(f"🥇 TOP PERFORMER: {top['symbol']}")
        print(f"   Name: {top['name']}")
        print(f"   Price: {top['price']}")
        print(f"   Gain: {top['pct_change']}")
        print("=" * 60)
        
    else:
        print("\n⚠️ Could not fetch data from Yahoo Finance")
        print("Trying TradingView...")
        
        gainers = get_market_summary()
        if gainers:
            print("\n🏆 Top Gainers from TradingView:\n")
            for i, g in enumerate(gainers, 1):
                print(f"  {i}. {g['symbol']} - {g['pct']}")
        else:
            print("\n❌ Could not fetch data. Try again later or check manually:")
            print("   - Yahoo Finance: https://finance.yahoo.com/markets/tw/stocks/gainers/")
            print("   - TradingView: https://www.tradingview.com/markets/stocks/taiwan/")

if __name__ == "__main__":
    main()
