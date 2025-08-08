import requests
import sqlite3
import logging
import time
from datetime import datetime, timedelta
import sys
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


with open("keys.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
API_KEY = data.get('coingecko_api_key')
def create_database():
    try:
        conn = sqlite3.connect('data/crypto_data.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bitcoin (
                date TEXT PRIMARY KEY,
                price REAL,
                volume REAL,
                market_cap REAL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ethereum (
                date TEXT PRIMARY KEY,
                price REAL,
                volume REAL,
                market_cap REAL
            )
        ''')
        
        conn.commit()
        logger.info("Database and tables created successfully")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise

def fetch_crypto_data(coin_id, days=30):
    endpoint = f"/coins/{coin_id}/market_chart"
    params = {
        'vs_currency': 'usd',
        'days': days,
        'interval': 'daily'
    }
    
    max_retries = 3
    retry_delay = 5  # sec
    
    for attempt in range(max_retries):
        try:
            response = requests.get(COINGECKO_API_URL + endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched data for {coin_id}")
            return data
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Rate limit exceeded
                logger.warning(f"Rate limit exceeded for {coin_id}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            logger.error(f"HTTP error fetching data for {coin_id}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {coin_id}: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)
    
    return None

def store_data(conn, coin_id, data):
    try:
        cursor = conn.cursor()
        table_name = coin_id
        
        for price, volume, market_cap in zip(
            data['prices'], data['total_volumes'], data['market_caps']
        ):
            timestamp_ms, value = price
            date = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d')
            
            cursor.execute(f'''
                INSERT OR REPLACE INTO {table_name} (date, price, volume, market_cap)
                VALUES (?, ?, ?, ?)
            ''', (date, value, volume[1], market_cap[1]))
        
        conn.commit()
        logger.info(f"Data stored successfully for {coin_id}")
    except sqlite3.Error as e:
        logger.error(f"Error storing data for {coin_id}: {e}")
        raise

# Main
def main():
    coins = ['bitcoin', 'ethereum']
    days = 30
    
    try:
        conn = create_database()
        
        for coin in coins:
            logger.info(f"Processing {coin}...")
            data = fetch_crypto_data(coin, days)
            if data:
                store_data(conn, coin, data)
            else:
                logger.error(f"No data received for {coin}")
            
            time.sleep(1)
        
        logger.info("Data collection completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()
