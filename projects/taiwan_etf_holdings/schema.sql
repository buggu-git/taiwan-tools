-- PostgreSQL Schema for Taiwan ETF Holdings Tracker

-- Drop existing tables if needed
DROP TABLE IF EXISTS etf_scrape_log CASCADE;
DROP TABLE IF EXISTS etf_holding_changes CASCADE;
DROP TABLE IF EXISTS etf_holdings CASCADE;
DROP TABLE IF EXISTS etf_master CASCADE;

-- ETF Master Table
CREATE TABLE etf_master (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    provider VARCHAR(50),
    type VARCHAR(50),
    zyte_url TEXT,
    listing_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Holdings Table (daily snapshot)
CREATE TABLE etf_holdings (
    id BIGSERIAL PRIMARY KEY,
    etf_symbol VARCHAR(10) NOT NULL REFERENCES etf_master(symbol) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    holding_date DATE,
    rank INTEGER,
    isin VARCHAR(12),
    cusip VARCHAR(9),
    issuer_name VARCHAR(255),
    security_name VARCHAR(255),
    security_type VARCHAR(50),
    shares_held DECIMAL(20, 4),
    market_value_twd DECIMAL(20, 2),
    market_value_usd DECIMAL(20, 2),
    weight_pct DECIMAL(10, 4),
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(etf_symbol, trade_date, isin)
);

-- Holdings Changes Tracking
CREATE TABLE etf_holding_changes (
    id BIGSERIAL PRIMARY KEY,
    etf_symbol VARCHAR(10) NOT NULL REFERENCES etf_master(symbol) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    previous_weight DECIMAL(10, 4),
    current_weight DECIMAL(10, 4),
    weight_change DECIMAL(10, 4),
    previous_shares DECIMAL(20, 4),
    current_shares DECIMAL(20, 4),
    shares_change DECIMAL(20, 4),
    change_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Scrape History
CREATE TABLE etf_scrape_log (
    id BIGSERIAL PRIMARY KEY,
    etf_symbol VARCHAR(10) NOT NULL REFERENCES etf_master(symbol) ON DELETE CASCADE,
    scrape_date DATE NOT NULL,
    scrape_start TIMESTAMP DEFAULT NOW(),
    scrape_end TIMESTAMP,
    status VARCHAR(20),
    error_message TEXT,
    pages_scraped INTEGER DEFAULT 0,
    holdings_count INTEGER DEFAULT 0,
    zyte_request_id VARCHAR(100)
);

-- Indexes
CREATE INDEX idx_holdings_etf_date ON etf_holdings(etf_symbol, trade_date DESC);
CREATE INDEX idx_holdings_date ON etf_holdings(trade_date DESC);
CREATE INDEX idx_holdings_isin ON etf_holdings(isin);
CREATE INDEX idx_changes_etf_date ON etf_holding_changes(etf_symbol, trade_date DESC);
CREATE INDEX idx_scrape_log ON etf_scrape_log(etf_symbol, scrape_date DESC);

COMMENT ON TABLE etf_master IS 'Master table for tracked ETFs';
COMMENT ON TABLE etf_holdings IS 'Daily holdings snapshots';
COMMENT ON TABLE etf_holding_changes IS 'Change detection log';
COMMENT ON TABLE etf_scrape_log IS 'Scraping operation history';
