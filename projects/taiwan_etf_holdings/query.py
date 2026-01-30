#!/usr/bin/env python3
"""
Query Taiwan ETF Holdings from PostgreSQL
"""

import yaml
import psycopg2
import pandas as pd
from datetime import date, timedelta
from typing import Optional
import os

def get_db_connection():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    db = config['database']
    return psycopg2.connect(
        host=db['host'],
        port=db['port'],
        database=db['name'],
        user=db['user'],
        password=os.getenv('DB_PASSWORD', db.get('password', ''))
    )

def query_holdings(etf_symbol: str, days: int = 7) -> pd.DataFrame:
    """Get recent holdings for an ETF"""
    conn = get_db_connection()
    df = pd.read_sql_query('''
        SELECT 
            trade_date, 
            rank, 
            issuer_name, 
            security_name, 
            shares_held, 
            ROUND(weight_pct * 100, 2) as weight_pct,
            market_value_twd
        FROM etf_holdings
        WHERE etf_symbol = %s
        ORDER BY trade_date DESC, rank ASC
        LIMIT %s
    ''', conn, params=(etf_symbol, days * 100))
    conn.close()
    return df

def query_changes(etf_symbol: str, days: int = 7) -> pd.DataFrame:
    """Get recent changes for an ETF"""
    conn = get_db_connection()
    df = pd.read_sql_query('''
        SELECT 
            trade_date,
            isin,
            change_type,
            ROUND(shares_change, 0) as shares_change,
            ROUND(weight_change * 100, 4) as weight_change_pct
        FROM etf_holding_changes
        WHERE etf_symbol = %s
        ORDER BY trade_date DESC
        LIMIT %s
    ''', conn, params=(etf_symbol, days * 50))
    conn.close()
    return df

def query_etfs() -> pd.DataFrame:
    """List all tracked ETFs"""
    conn = get_db_connection()
    df = pd.read_sql_query('SELECT symbol, name, provider, type FROM etf_master', conn)
    conn.close()
    return df

def query_scrape_log(etf_symbol: Optional[str] = None, days: int = 7) -> pd.DataFrame:
    """Get scrape history"""
    conn = get_db_connection()
    if etf_symbol:
        df = pd.read_sql_query('''
            SELECT 
                scrape_date,
                etf_symbol,
                status,
                holdings_count,
                error_message
            FROM etf_scrape_log
            WHERE etf_symbol = %s
            ORDER BY scrape_date DESC
            LIMIT %s
        ''', conn, params=(etf_symbol, days))
    else:
        df = pd.read_sql_query('''
            SELECT 
                scrape_date,
                etf_symbol,
                status,
                holdings_count,
                error_message
            FROM etf_scrape_log
            ORDER BY scrape_date DESC
            LIMIT %s
        ''', conn, params=(days,))
    conn.close()
    return df

def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python query.py <command> [args]")
        print("Commands:")
        print("  list                      - Show all tracked ETFs")
        print("  holdings <symbol> [days]  - Show holdings (default: 7 days)")
        print("  changes <symbol> [days]   - Show changes (default: 7 days)")
        print("  log [symbol] [days]       - Show scrape log")
        return
    
    cmd = sys.argv[1]
    
    if cmd == 'list':
        print("\n📊 Tracked ETFs:")
        df = query_etfs()
        print(df.to_string(index=False))
    
    elif cmd == 'holdings':
        symbol = sys.argv[2] if len(sys.argv) > 2 else '00919'
        days = int(sys.argv[3]) if len(sys.argv) > 3 else 7
        print(f"\n📈 Holdings for {symbol} (last {days} days):")
        df = query_holdings(symbol, days)
        if df.empty:
            print("No data found. Run scraper first!")
        else:
            print(df.to_string(index=False))
    
    elif cmd == 'changes':
        symbol = sys.argv[2] if len(sys.argv) > 2 else '00919'
        days = int(sys.argv[3]) if len(sys.argv) > 3 else 7
        print(f"\n🔄 Changes for {symbol} (last {days} days):")
        df = query_changes(symbol, days)
        if df.empty:
            print("No changes detected")
        else:
            print(df.to_string(index=False))
    
    elif cmd == 'log':
        symbol = sys.argv[2] if len(sys.argv) > 2 else None
        days = int(sys.argv[3]) if len(sys.argv) > 3 else 7
        print(f"\n📋 Scrape Log:")
        df = query_scrape_log(symbol, days)
        print(df.to_string(index=False))

if __name__ == "__main__":
    main()
