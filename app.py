from flask import Flask, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import send_from_directory
import os

DB_CONFIG = {
    "dbname": "weather_db",
    "user": "postgres",
    "password": "senuja123",
    "host": "localhost",
    "port": 5432,
}

app = Flask(__name__)

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

@app.route("/api/weather/latest")
def latest_weather():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM weather_current
        ORDER BY weather_time DESC
        LIMIT 1;
        """
    )
    row = cur.fetchone()
    conn.close()
    return jsonify(row)

@app.route("/api/weather/history")
def weather_history():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
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
    return jsonify(rows)

@app.route("/")
def dashboard():
    return send_from_directory("static", "dashboard.html")

@app.route("/api/weather/stats")
def weather_stats():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM weather_stats
        ORDER BY calculated_at DESC
        LIMIT 10;
        """
    )
    rows = cur.fetchall()
    return jsonify(rows)

if __name__ == "__main__":
    app.run(debug=True)