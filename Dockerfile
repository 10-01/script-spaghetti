FROM python:3.13-slim

# Install libpq for psycopg2
RUN apt-get update && apt-get install -y libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

RUN chmod +x run_daily.sh

CMD ["bash", "run_daily.sh"]
