FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \ 
&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV NEW_RELIC_CONFIG_FILE=/app/newrelic.ini
ENV NEW_RELIC_ENV=production

EXPOSE 8080

CMD ["newrelic-admin", "run-program", "uvicorn", "server.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]