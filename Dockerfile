FROM python:3.13-slim

# Рабочая папка внутри контейнера
WORKDIR /app

# Копируем список библиотек и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ваш скрипт
COPY config.py .
COPY weather_etl.py .

# Указываем переменную для Prefect API (внутри сети Docker)
ENV PREFECT_API_URL="http://prefect-server:4200/api"

# Запускаем
CMD ["python", "weather_etl.py"]