import pandas as pd
import sqlite3
import logging
import sys
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

os.makedirs('visualizations', exist_ok=True)

def load_data():
    try:
        conn = sqlite3.connect('data/crypto_data.db')
        btc_df = pd.read_sql_query("SELECT * FROM bitcoin", conn, parse_dates=['date'])
        eth_df = pd.read_sql_query("SELECT * FROM ethereum", conn, parse_dates=['date'])
        conn.close()
        logger.info("Data loaded successfully from SQLite")
        return btc_df, eth_df
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise

def clean_data(df, coin_name):
    try:
        if df.isnull().any().any():
            logger.warning(f"Missing values found in {coin_name} data")
            df = df.interpolate(method='linear', limit_direction='both')
            logger.info(f"Missing values in {coin_name} interpolated")

        if (df['price'] <= 0).any() or (df['volume'] <= 0).any():
            logger.warning(f"Anomalies detected in {coin_name} data")
            df = df[(df['price'] > 0) & (df['volume'] > 0)]

        df = df.sort_values('date')
        return df
    except Exception as e:
        logger.error(f"Error cleaning data for {coin_name}: {e}")
        raise

# Function to analyze data
def analyze_data(btc_df, eth_df):
    analysis = {}
    
    # Calculate metrics for each coin
    for df, coin in [(btc_df, 'Bitcoin'), (eth_df, 'Ethereum')]:
        analysis[coin] = {
            'mean_price': df['price'].mean(),
            'volatility': df['price'].std() / df['price'].mean() * 100,  # CV in %
            'price_change_pct': ((df['price'].iloc[-1] - df['price'].iloc[0]) / df['price'].iloc[0]) * 100,
            'avg_volume': df['volume'].mean()
        }
    
    # Correlation
    merged_df = pd.merge(btc_df[['date', 'price']], eth_df[['date', 'price']], 
                        on='date', suffixes=('_btc', '_eth'))
    correlation = merged_df['price_btc'].corr(merged_df['price_eth'])
    analysis['correlation'] = correlation

    # SQL aggregation: avg by weekday
    conn = sqlite3.connect('data/crypto_data.db')
    btc_df['weekday'] = btc_df['date'].dt.day_name()
    eth_df['weekday'] = eth_df['date'].dt.day_name()
    btc_weekday_avg = pd.read_sql_query("""
        SELECT strftime('%w', date) as weekday_num, 
               CASE strftime('%w', date)
                   WHEN '0' THEN 'Sunday'
                   WHEN '1' THEN 'Monday'
                   WHEN '2' THEN 'Tuesday'
                   WHEN '3' THEN 'Wednesday'
                   WHEN '4' THEN 'Thursday'
                   WHEN '5' THEN 'Friday'
                   WHEN '6' THEN 'Saturday'
               END as weekday,
               AVG(price) as avg_price,
               AVG(volume) as avg_volume
        FROM bitcoin
        GROUP BY weekday_num
        ORDER BY weekday_num
    """, conn)
    conn.close()
    
    analysis['btc_weekday_trends'] = btc_weekday_avg
    return analysis


def visualize_data(btc_df, eth_df, analysis):
    plt.style.use('seaborn-v0_8')  

    #lineplot
    plt.figure(figsize=(12, 6))
    plt.plot(btc_df['date'], btc_df['price'], label='Bitcoin', color='orange')
    plt.plot(eth_df['date'], eth_df['price'], label='Ethereum', color='blue')
    plt.title('Bitcoin and Ethereum Prices Over Time')
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('visualizations/price_trend.png')
    plt.close()
    logger.info("Price trend plot saved")

    #heatmap of correlations
    merged_df = pd.merge(btc_df[['date', 'price']], eth_df[['date', 'price']], 
                        on='date', suffixes=('_btc', '_eth'))
    corr_matrix = merged_df[['price_btc', 'price_eth']].corr()
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
    plt.title('Correlation Matrix: Bitcoin vs Ethereum Prices')
    plt.savefig('visualizations/correlation_heatmap.png')
    plt.close()
    logger.info("Correlation heatmap saved")

    #week
    btc_weekday_avg = analysis['btc_weekday_trends']
    plt.figure(figsize=(10, 6))
    plt.bar(btc_weekday_avg['weekday'], btc_weekday_avg['avg_volume'], color='green')
    plt.title('Average Bitcoin Trading Volume by Weekday')
    plt.xlabel('Weekday')
    plt.ylabel('Average Volume (USD)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('visualizations/volume_bar.png')
    plt.close()
    logger.info("Volume bar plot saved")

def export_to_excel(btc_df, eth_df):
    with pd.ExcelWriter('visualizations/crypto_data.xlsx') as writer:
        btc_df.to_excel(writer, sheet_name='Bitcoin', index=False)
        eth_df.to_excel(writer, sheet_name='Ethereum', index=False)
    logger.info("Data exported to Excel")

#Markdown
def generate_report(analysis):
    report = """
# Cryptocurrency Analysis Report

## Summary
- **Bitcoin**:
  - Average Price: ${:.2f}
  - Volatility: {:.2f}%
  - Price Change (30 days): {:.2f}%
  - Average Volume: ${:.2f}
- **Ethereum**:
  - Average Price: ${:.2f}
  - Volatility: {:.2f}%
  - Price Change (30 days): {:.2f}%
  - Average Volume: ${:.2f}
- **Correlation**: Bitcoin and Ethereum prices correlate at {:.2f}

## Trends
- Bitcoin trading volume is highest on {} (avg: ${:.2f}).
- Bitcoin trading volume is lowest on {} (avg: ${:.2f}).

## Business Recommendations
- Monitor trading volumes on high-volume days ({}) for potential price movements.
- Use the high correlation ({:.2f}) between Bitcoin and Ethereum for portfolio diversification strategies.
- Track weekend volatility for trading opportunities.

## Visualizations
- Price trends: See `visualizations/price_trend.png`
- Correlation heatmap: See `visualizations/correlation_heatmap.png`
- Volume by weekday: See `visualizations/volume_bar.png`
""".format(
        analysis['Bitcoin']['mean_price'],
        analysis['Bitcoin']['volatility'],
        analysis['Bitcoin']['price_change_pct'],
        analysis['Bitcoin']['avg_volume'],
        analysis['Ethereum']['mean_price'],
        analysis['Ethereum']['volatility'],
        analysis['Ethereum']['price_change_pct'],
        analysis['Ethereum']['avg_volume'],
        analysis['correlation'],
        analysis['btc_weekday_trends'].iloc[analysis['btc_weekday_trends']['avg_volume'].idxmax()]['weekday'],
        analysis['btc_weekday_trends']['avg_volume'].max(),
        analysis['btc_weekday_trends'].iloc[analysis['btc_weekday_trends']['avg_volume'].idxmin()]['weekday'],
        analysis['btc_weekday_trends']['avg_volume'].min(),
        analysis['btc_weekday_trends'].iloc[analysis['btc_weekday_trends']['avg_volume'].idxmax()]['weekday'],
        analysis['correlation']
    )
    
    with open('visualizations/report.md', 'w') as f:
        f.write(report)
    logger.info("Report generated")

# Main
def main():
    try:
        btc_df, eth_df = load_data()
        
        btc_df = clean_data(btc_df, 'Bitcoin')
        eth_df = clean_data(eth_df, 'Ethereum')
        
        analysis = analyze_data(btc_df, eth_df)
        
        visualize_data(btc_df, eth_df, analysis)
        
        export_to_excel(btc_df, eth_df)
        
        generate_report(analysis)
        
        logger.info("Analysis and visualization completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()