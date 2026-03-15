import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectTimeout, ReadTimeout, RequestException
from urllib3.util.retry import Retry

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


# 2. EXTRACT: call API and get JSON with retries + longer timeout
def extract():
    # Configure retries on network/server errors
    retry_strategy = Retry(
        total=5,                  # up to 5 attempts
        backoff_factor=1,         # 1s, 2s, 4s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        print(f"Requesting: {API_URL}")
        resp = session.get(API_URL, timeout=20)  # increased timeout
        resp.raise_for_status()
        print("API call successful.")
        return resp.json()
    except (ConnectTimeout, ReadTimeout) as e:
        print(f"Timeout calling Open-Meteo: {e}")
        return None
    except RequestException as e:
        print(f"HTTP error calling Open-Meteo: {e}")
        return None


# 3. TRANSFORM: map JSON -> Python tuple matching table schema
def transform(api_data):
    if api_data is None:
        return []

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
    if not rows:
        print("No rows to insert, skipping DB load.")
        return

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
    if data is None:
        print("Extract failed, ETL stopped before transform/load.")
        return

    rows = transform(data)
    if not rows:
        print("Transform produced no rows, ETL stopped before load.")
        return

    load(rows)
    print("ETL finished.")


if __name__ == "__main__":
    run()
