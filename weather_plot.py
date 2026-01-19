import psycopg2
import matplotlib.pyplot as plt
from datetime import datetime

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

    cur.execute(
        """
        SELECT weather_time, temperature
        FROM weather_current
        WHERE weather_time >= NOW() - INTERVAL '24 hours'
        ORDER BY weather_time;
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No data to plot.")
        return

    times = [r[0] for r in rows]
    temps = [r[1] for r in rows]

    plt.figure(figsize=(10, 4))
    plt.plot(times, temps, marker="o")
    plt.title("Temperature - last 24 hours")
    plt.xlabel("Time")
    plt.ylabel("Temperature (°C)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
