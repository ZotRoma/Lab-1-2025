import requests
import pandas as pd
import json
from datetime import date, timedelta, datetime
from io import BytesIO

from prefect import flow, task, get_run_logger
from minio import Minio
import clickhouse_connect
import config


import os
# если код внутри Docker, он использует специальное имя 'host.docker.internal'
# чтобы достучаться до других контейнеров.
HOST = os.getenv('DOCKER_HOST_INTERNAL', 'localhost')


TG_BOT_TOKEN = config.TOKEN_BOT
TG_CHAT_ID = config.ID_BOT

CLICKHOUSE_HOST  = HOST
MINIO_ENDPOINT = f"{HOST}:9000"

CITIES = {
    "Moscow": {"lat": 55.7558, "lon": 37.6173},
    "Samara": {"lat": 53.2001, "lon": 50.15},
}

# --- ЗАДАЧИ (TASKS) ---
# @task — декоратор Prefect, который делает из функции "задачу".
# retries=3 — если задача упадет (например, нет сети), Prefect попробует ее перезапустить 3 раза.

@task(name="Init DB & MinIO", retries=3)
def init_infrastructure():
    """Создает бакет и таблицы, если они не существуют"""
    logger = get_run_logger()
    
    # Настройка MinIO
    client = Minio(endpoint=MINIO_ENDPOINT, 
                   access_key=config.MINIO_ACCESS_KEY, 
                   secret_key=config.MINIO_SECRET_KEY, 
                   secure=False
                   )
    
    # Проверяем, есть ли bucket. Если нет — создаем.
    if not client.bucket_exists(bucket_name=config.MINIO_BUCKET):
        client.make_bucket(bucket_name=config.MINIO_BUCKET)
        logger.info(f"Bucket {config.MINIO_BUCKET} created.")

    # Настройка ClickHouse
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, 
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER, 
        password=config.CLICKHOUSE_PASSWORD
    )
    
    # Создаем таблицу для почасовых данных, если её нет.
    # ENGINE = ReplacingMergeTree(). перезаписывает старые данные на новые (удаляет дубликаты)
    ch_client.command("""
    CREATE TABLE IF NOT EXISTS weather_hourly (
        city String,
        forecast_time DateTime,
        temperature Float32,
        precipitation Float32,
        wind_speed Float32,
        wind_direction Int16
    ) ENGINE = ReplacingMergeTree() ORDER BY (city, forecast_time)
    """)
    # Создаем таблицу для дневной статистики
    ch_client.command("""
    CREATE TABLE IF NOT EXISTS weather_daily (
        city String,
        date Date,
        min_temp Float32,
        max_temp Float32,
        avg_temp Float32,
        total_precip Float32
    ) ENGINE = ReplacingMergeTree() ORDER BY (city, date)
    """)
    logger.info("ClickHouse tables initialized.")

# Получаем погоду на завтра
@task(name="Extract Forecast", retries=3, retry_delay_seconds=10)
def extract_weather(city_name: str, lat: float, lon: float):
    
    logger = get_run_logger()
    
    # когда это "завтра"
    tomorrow = date.today() + timedelta(days=1)

    # Формируем запрос к Open-Meteo
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,wind_direction_10m",
        "start_date": tomorrow,
        "end_date": tomorrow,
        "timezone": "auto"
    }
    
    # Делаем HTTP запрос
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    logger.info(f"Data fetched for {city_name} on {tomorrow}")
    return data, tomorrow

@task(name="Load to MinIO")
def save_to_minio(city_name: str, data: dict, forecast_date: date):
    # Сохранение 'сырых' данных. Не теряем исходники
    client = Minio(endpoint=MINIO_ENDPOINT, access_key=config.MINIO_ACCESS_KEY, secret_key=config.MINIO_SECRET_KEY, secure=False)
    
    # Превращаем словарь данных обратно в JSON-строку и в байты
    json_data = json.dumps(data).encode('utf-8')
    # Формируем имя файла
    file_name = f"{forecast_date}/{city_name}.json"
    
    # Загружаем файл в хранилище
    client.put_object(
        bucket_name=config.MINIO_BUCKET,
        object_name=file_name,
        data=BytesIO(json_data),
        length=len(json_data),
        content_type='application/json'
    )
    get_run_logger().info(f"Saved {file_name} to MinIO")

@task(name="Transform Hourly")
def process_hourly(city_name: str, data: dict):
    hourly = data['hourly']

    # Создаем таблицу в Pandas
    df = pd.DataFrame({
        'city': city_name,
        'forecast_time': pd.to_datetime(hourly['time']),
        'temperature': hourly['temperature_2m'],
        'precipitation': hourly['precipitation'],
        'wind_speed': hourly['wind_speed_10m'],
        'wind_direction': hourly['wind_direction_10m']
    })
    return df

@task(name="Transform Daily")
def process_daily(city_name: str, hourly_df: pd.DataFrame):
    # Агрегируем данные, для дневной статистики
    daily_stats = {
        'city': city_name,
        'date': hourly_df['forecast_time'].dt.date.iloc[0],
        'min_temp': hourly_df['temperature'].min(),
        'max_temp': hourly_df['temperature'].max(),
        'avg_temp': round(hourly_df['temperature'].mean(), 1),
        'total_precip': round(hourly_df['precipitation'].sum(), 1)
    }
    return daily_stats

@task(name="Load ClickHouse")
def load_to_clickhouse(hourly_df: pd.DataFrame, daily_stats: dict):
    #Загружаем данные в ClickHouse
    
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, 
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER, 
        password=config.CLICKHOUSE_PASSWORD
    )   
    # insert_df — удобный метод библиотеки, который сам вставляет DataFrame в таблицу
    # почасовые данные
    client.insert_df('weather_hourly', hourly_df)
    
    # ежедневные данные
    daily_df = pd.DataFrame([daily_stats])
    client.insert_df('weather_daily', daily_df)
    
    get_run_logger().info(f"Inserted data for {daily_stats['city']} into ClickHouse")

@task(name="Send Telegram")
def send_telegram_alert(daily_stats: dict):
    #Отправляем уведомление в Telegram
    logger = get_run_logger()
    
    # проверочка
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        logger.warning("Учётные данные Telegram не установлены. Пропуск уведомлений.")
        return
    
    # формируем красивое сообщение 
    msg = (
        f"🌤 **Прогноз для {daily_stats['city']} на {daily_stats['date']}**\n\n"
        f"🌡 Температура: от {daily_stats['min_temp']}°C до {daily_stats['max_temp']}°C\n"
        f"💧 Осадки: {daily_stats['total_precip']} мм\n"
    )
    
    # дополнительные предупреждения
    if daily_stats['total_precip'] > 5.0:
        msg += "\n⚠️ Внимание: Ожидаются сильные осадки!"
    if daily_stats['min_temp'] < -20:
        msg += "\n⚠️ Внимание: Сильный мороз!"

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    # отправляем запрос    
    try:
        # POST запрос в API Telegram
        response = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"})
        
        # проверяем, что сервер ответил 200 OK.
        response.raise_for_status()
        
        logger.info("Уведомление в Telegram отправлено.")
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение Telegram: {e}")



@flow(name="weather_etl_flow")
def weather_etl():
    # 0. Инициализация
    init_infrastructure()
    
    # цикл по городам
    for city, coords in CITIES.items():
        # 1. получение данных
        data, forecast_date = extract_weather(city, coords['lat'], coords['lon'])
        
        # 2. сохранение в MinIO
        save_to_minio(city, data, forecast_date)
        
        # 3. трансформация
        hourly_df = process_hourly(city, data)
        daily_stats = process_daily(city, hourly_df)
        
        # 4. сохранение в ClickHouse
        load_to_clickhouse(hourly_df, daily_stats)
        
        # 5. отправка сообщения в телеграм
        send_telegram_alert(daily_stats)

if __name__ == "__main__":
    # .serve запускает программу в режиме "Сервера".
    # программа не завершится, а будет висеть и ждать времени запуска по расписанию.
    weather_etl.serve(
        name="daily-weather-cron",
        cron="30 14 * * *", # расписание отправки сообщения в формате CRON
        tags=["lab", "scheduled"],
        description="Запуск ETL пайплайна каждый вечер в 18:30"
    )