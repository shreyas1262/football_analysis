FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "mcp[cli]>=1.0.0" \
    "psycopg2-binary>=2.9.9" \
    "python-dotenv>=1.0.0" \
    "uvicorn>=0.30.0" \
    "starlette>=0.40.0" \
    "anyio>=4.0.0" \
    "voyageai>=0.3.0" \
    "anthropic>=0.40.0"

COPY mcp/ ./mcp/
COPY config/ ./config/
COPY agent/ ./agent/
COPY mcp_entrypoint.py ./

EXPOSE 8000

CMD ["python", "mcp/football_mcp_server.py"]
