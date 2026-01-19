import psycopg2

DB_CONFIG = {
    "dbname": "weather_db",          # or weather_db if you already created it
    "user": "postgres",            # your pg username
    "password": "senuja123",   # the one you set during install
    "host": "localhost",
    "port": 5432,
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print("Connected! PostgreSQL version:", version)
    cur.close()
    conn.close()
except Exception as e:
    print("Connection error:", e)
