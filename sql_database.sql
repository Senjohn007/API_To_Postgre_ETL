CREATE TABLE weather_current (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100),
    latitude NUMERIC,
    longitude NUMERIC,
    temperature NUMERIC,
    windspeed NUMERIC,
    winddirection NUMERIC,
    is_day SMALLINT,
    weather_time TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE weather_stats (
    id SERIAL PRIMARY KEY,
    from_time TIMESTAMP,
    to_time   TIMESTAMP,
    rows_count INTEGER,
    min_temp NUMERIC,
    max_temp NUMERIC,
    avg_temp NUMERIC,
    min_windspeed NUMERIC,
    max_windspeed NUMERIC,
    avg_windspeed NUMERIC,
    calculated_at TIMESTAMP DEFAULT NOW()
);


select * from weather_current;


select * from weather_stats;