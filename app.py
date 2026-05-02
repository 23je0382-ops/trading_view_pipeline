import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from .env file (for local development)
load_dotenv()

app = Flask(__name__)

# PostgreSQL Configuration
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn is None:
        print("WARNING: DATABASE_URL environment variable not set. Database not initialized.")
        return
        
    try:
        with conn.cursor() as cur:
            # Create the tables if they don't exist
            tables = ['trading_alerts', 'alerts_15m', 'alerts_30m']
            for table in tables:
                cur.execute(f'''
                    CREATE TABLE IF NOT EXISTS {table} (
                        id SERIAL PRIMARY KEY,
                        ticker VARCHAR(50) NOT NULL,
                        time VARCHAR(50),
                        open NUMERIC,
                        high NUMERIC,
                        low NUMERIC,
                        close NUMERIC,
                        volume NUMERIC,
                        interval VARCHAR(50),
                        exchange VARCHAR(50),
                        is_aggregated BOOLEAN DEFAULT FALSE,
                        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Safely add the new columns to existing tables
                columns = [
                    ('interval', 'VARCHAR(50)'),
                    ('exchange', 'VARCHAR(50)'),
                    ('is_aggregated', 'BOOLEAN DEFAULT FALSE')
                ]
                for col_name, col_type in columns:
                    try:
                        cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type};')
                    except Exception:
                        pass

        conn.commit()
        print("Connected to PostgreSQL and verified tables exist!")
    except Exception as e:
        print(f"Error initializing database table: {e}")
    finally:
        conn.close()

def aggregate_ohlcv(rows, target_interval):
    if not rows:
        return None
    
    # Sort rows by time or ID to ensure correct open/close
    sorted_rows = sorted(rows, key=lambda x: x['id'])
    
    return {
        'ticker': sorted_rows[0]['ticker'],
        'time': sorted_rows[-1]['time'],
        'open': sorted_rows[0]['open'],
        'high': max(row['high'] for row in sorted_rows),
        'low': min(row['low'] for row in sorted_rows),
        'close': sorted_rows[-1]['close'],
        'volume': sum(row['volume'] for row in sorted_rows),
        'interval': target_interval,
        'exchange': sorted_rows[0]['exchange']
    }

def check_and_aggregate(source_table, target_table, count_required, target_interval):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        aggregated_any = False
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get symbols that have un-aggregated rows
            cur.execute(f"SELECT DISTINCT ticker FROM {source_table} WHERE is_aggregated = FALSE")
            tickers = [row['ticker'] for row in cur.fetchall()]
            
            for ticker in tickers:
                # Fetch the un-aggregated rows for this ticker
                cur.execute(f'''
                    SELECT * FROM {source_table} 
                    WHERE ticker = %s AND is_aggregated = FALSE 
                    ORDER BY id ASC 
                    LIMIT %s
                ''', (ticker, count_required))
                
                rows = cur.fetchall()
                
                if len(rows) == count_required:
                    # Perform aggregation
                    aggregated_data = aggregate_ohlcv(rows, target_interval)
                    
                    # Insert into target table
                    cur.execute(f'''
                        INSERT INTO {target_table} (ticker, time, open, high, low, close, volume, interval, exchange)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        aggregated_data['ticker'], aggregated_data['time'], 
                        aggregated_data['open'], aggregated_data['high'], 
                        aggregated_data['low'], aggregated_data['close'], 
                        aggregated_data['volume'], aggregated_data['interval'], 
                        aggregated_data['exchange']
                    ))
                    
                    # Mark source rows as aggregated
                    row_ids = [row['id'] for row in rows]
                    cur.execute(f"UPDATE {source_table} SET is_aggregated = TRUE WHERE id = ANY(%s)", (row_ids,))
                    
                    print(f"Aggregated {count_required} rows from {source_table} into {target_table} for {ticker}")
                    aggregated_any = True
                    
                    # If we aggregated into 15m, trigger 30m check immediately
                    if target_table == 'alerts_15m':
                        check_and_aggregate('alerts_15m', 'alerts_30m', 2, '30')
        
        conn.commit()
        return aggregated_any
    except Exception as e:
        print(f"Error during aggregation: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def backfill_on_startup():
    print("Starting automatic backfill...")
    # Loop 5m -> 15m until no more can be aggregated
    while check_and_aggregate('trading_alerts', 'alerts_15m', 3, '15'):
        pass
    # Loop 15m -> 30m until no more can be aggregated
    while check_and_aggregate('alerts_15m', 'alerts_30m', 2, '30'):
        pass
    print("Backfill completed!")

# Initialize and backfill when the app starts
init_db()
backfill_on_startup()

def write_to_postgres(data):
    conn = get_db_connection()
    if conn is None:
        raise Exception("PostgreSQL is not configured. Please set DATABASE_URL.")
        
    try:
        with conn.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO trading_alerts (ticker, time, open, high, low, close, volume, interval, exchange)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (
                    data.get('symbol', data.get('ticker', 'UNKNOWN')),
                    data.get('time', ''),
                    data.get('open', None),
                    data.get('high', None),
                    data.get('low', None),
                    data.get('close', None),
                    data.get('volume', None),
                    data.get('interval', None),
                    data.get('exchange', None)
                )
            )
        conn.commit()
        
        # Trigger aggregation chain
        check_and_aggregate('trading_alerts', 'alerts_15m', 3, '15')
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        try:
            data = request.json
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    pass
            
            if not data:
                return jsonify({"error": "No JSON data received"}), 400
                
            print(f"Received alert: {data}")
            write_to_postgres(data)
            
            return jsonify({"status": "success", "message": "Data saved and aggregated"}), 200
            
        except Exception as e:
            print(f"Error processing webhook: {str(e)}")
            return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Run the app locally on port 5001
    app.run(host='0.0.0.0', port=5001, debug=True)
