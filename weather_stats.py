import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    "dbname": "weather_db",
    "user": "postgres",
    "password": "senuja123",
    "host": "localhost",
    "port": 5432,
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # last 24 hours (you can change interval)
    cur.execute(
        """
        SELECT weather_time, temperature, windspeed
        FROM weather_current
        WHERE weather_time >= NOW() - INTERVAL '24 hours'
        ORDER BY weather_time;
        """
    )
    rows = cur.fetchall()
    conn.close()

    print(f"Rows in last 24h: {len(rows)}")
    if not rows:
        return

    temps = [r[1] for r in rows]
    winds = [r[2] for r in rows]

    print(f"Min temp: {min(temps):.2f}")
    print(f"Max temp: {max(temps):.2f}")
    print(f"Avg temp: {sum(temps) / len(temps):.2f}")

    print(f"Min windspeed: {min(winds):.2f}")
    print(f"Max windspeed: {max(winds):.2f}")
    print(f"Avg windspeed: {sum(winds) / len(winds):.2f}")

if __name__ == "__main__":
    main()
