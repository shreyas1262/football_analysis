FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY mcp_entrypoint.py ./

ENV PYTHONPATH=/app/src

RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["python", "mcp_entrypoint.py"]
