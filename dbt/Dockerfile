FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc libc6-dev git && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir dbt-postgres==1.8.0
WORKDIR /usr/app/dbt
