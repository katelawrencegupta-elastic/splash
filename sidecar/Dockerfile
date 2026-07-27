FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
ENV PIP_REQUIRE_HASHES=0
RUN pip install --no-cache-dir -r requirements.txt

COPY classify.py streams.py app.py classify_rules.json ./

ENV PYTHONUNBUFFERED=1
# Single worker keeps classify LRU + _ensured cache process-local (no 4× cold ES PUTs).
ENV UVICORN_WORKERS=1
EXPOSE 8080

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port 8080 --workers ${UVICORN_WORKERS}"]
