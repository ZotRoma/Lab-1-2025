-- Таблица почасовых данных
CREATE TABLE IF NOT EXISTS weather_hourly (
    city String,
    forecast_time DateTime,
    temperature Float32,
    precipitation Float32,
    wind_speed Float32,
    wind_direction Int16
) ENGINE = ReplacingMergeTree()
ORDER BY (city, forecast_time);

-- Таблица дневных агрегатов
CREATE TABLE IF NOT EXISTS weather_daily (
    city String,
    date Date,
    min_temp Float32,
    max_temp Float32,
    avg_temp Float32,
    total_precip Float32
) ENGINE = ReplacingMergeTree()
ORDER BY (city, date);