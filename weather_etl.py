import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# 1. CONFIG
API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=6.9271&longitude=79.8612&current_weather=true"
)
CITY_NAME = "Colombo"

DB_CONFIG = {
    "dbname": "weather_db",
    "user": "postgres",      # change if different
    "password": "senuja123",
    "host": "localhost",
    "port": 5432,
}

# 2. EXTRACT: call API and get JSON
def extract():
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()  # raise error for 4xx/5xx
    return resp.json()

# 3. TRANSFORM: map JSON -> Python tuple matching table schema
def transform(api_data):
    lat = api_data["latitude"]
    lon = api_data["longitude"]
    cw = api_data["current_weather"]

    weather_time = datetime.fromisoformat(cw["time"])
    row = (
        CITY_NAME,
        lat,
        lon,
        cw["temperature"],
        cw["windspeed"],
        cw["winddirection"],
        cw["is_day"],
        weather_time,
    )
    return [row]  # list of rows

# 4. LOAD: insert into PostgreSQL
def load(rows):
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO weather_current (
                city_name, latitude, longitude,
                temperature, windspeed, winddirection,
                is_day, weather_time
            )
            VALUES %s;
        """

        execute_values(cur, insert_sql, rows)
        conn.commit()
        cur.close()
        print(f"Inserted {len(rows)} rows.")
    except Exception as e:
        print("Error during load:", e)
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()

# 5. MAIN: run ETL
def run():
    print("Starting ETL...")
    data = extract()
    rows = transform(data)
    load(rows)
    print("ETL finished.")

if __name__ == "__main__":
    run()
