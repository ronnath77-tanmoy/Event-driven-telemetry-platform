FROM python:3.12-slim

WORKDIR /app

# Copy requirements first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set New Relic config
ENV NEW_RELIC_CONFIG_FILE=/app/newrelic.ini

EXPOSE 8000

# Run app with New Relic
CMD ["newrelic-admin", "run-program", "uvicorn", "server.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]