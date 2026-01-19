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

    # define window: last 24 hours
    cur.execute("SELECT NOW() - INTERVAL '24 hours', NOW();")
    from_time, to_time = cur.fetchone()

    # fetch rows for that window
    cur.execute(
        """
        SELECT weather_time, temperature, windspeed
        FROM weather_current
        WHERE weather_time >= %s AND weather_time <= %s
        ORDER BY weather_time;
        """,
        (from_time, to_time),
    )
    rows = cur.fetchall()

    print(f"Rows in last 24h: {len(rows)}")
    if not rows:
        conn.close()
        return

    temps = [r[1] for r in rows]
    winds = [r[2] for r in rows]

    min_temp = min(temps)
    max_temp = max(temps)
    avg_temp = sum(temps) / len(temps)

    min_wind = min(winds)
    max_wind = max(winds)
    avg_wind = sum(winds) / len(winds)

    print(f"Min temp: {min_temp:.2f}")
    print(f"Max temp: {max_temp:.2f}")
    print(f"Avg temp: {avg_temp:.2f}")
    print(f"Min windspeed: {min_wind:.2f}")
    print(f"Max windspeed: {max_wind:.2f}")
    print(f"Avg windspeed: {avg_wind:.2f}")

    # insert stats row into weather_stats
    cur.execute(
        """
        INSERT INTO weather_stats (
            from_time, to_time, rows_count,
            min_temp, max_temp, avg_temp,
            min_windspeed, max_windspeed, avg_windspeed
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            from_time,
            to_time,
            len(rows),
            min_temp,
            max_temp,
            avg_temp,
            min_wind,
            max_wind,
            avg_wind,
        ),
    )

    conn.commit()
    conn.close()
    print("Stats row inserted into weather_stats.")

if __name__ == "__main__":
    main()
