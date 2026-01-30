#!/bin/bash
# Setup PostgreSQL for Taiwan ETF Holdings Tracker

set -e

DB_NAME="taiwan_etf_db"
DB_USER="postgres"

echo "=== Taiwan ETF Holdings - PostgreSQL Setup ==="

# Check if PostgreSQL is running
if ! pg_isready -q 2>/dev/null; then
    echo "PostgreSQL not running. Starting..."
    brew services start postgresql
    sleep 2
fi

# Create database
echo "Creating database: $DB_NAME"
psql -U $DB_USER -c "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null || true
psql -U $DB_USER -c "CREATE DATABASE $DB_NAME;"

# Apply schema
echo "Applying schema..."
psql -U $DB_USER -d $DB_NAME -f schema.sql

echo "=== PostgreSQL setup complete ==="
echo "Database: $DB_NAME"
echo "Run: export DB_PASSWORD='your_password' && python scraper.py"
