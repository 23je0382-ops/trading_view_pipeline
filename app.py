import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime

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
            # Create the table if it doesn't exist
            cur.execute('''
                CREATE TABLE IF NOT EXISTS trading_alerts (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(50) NOT NULL,
                    time VARCHAR(50),
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume NUMERIC,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        conn.commit()
        print("Connected to PostgreSQL and verified table exists!")
    except Exception as e:
        print(f"Error initializing database table: {e}")
    finally:
        conn.close()

# Initialize the database table when the app starts
init_db()

def write_to_postgres(data):
    conn = get_db_connection()
    if conn is None:
        raise Exception("PostgreSQL is not configured. Please set DATABASE_URL.")
        
    try:
        with conn.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO trading_alerts (ticker, time, open, high, low, close, volume, received_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (
                    data.get('ticker', 'UNKNOWN'),
                    data.get('time', ''),
                    data.get('open', None),
                    data.get('high', None),
                    data.get('low', None),
                    data.get('close', None),
                    data.get('volume', None),
                    datetime.utcnow()
                )
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        try:
            # Parse the incoming JSON data from TradingView
            data = request.json
            
            if not data:
                return jsonify({"error": "No JSON data received"}), 400
                
            print(f"Received alert: {data}")
            
            # Write to PostgreSQL
            write_to_postgres(data)
            
            return jsonify({"status": "success", "message": "Data saved to PostgreSQL"}), 200
            
        except Exception as e:
            print(f"Error processing webhook: {str(e)}")
            return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Run the app locally on port 5001
    app.run(host='0.0.0.0', port=5001, debug=True)
