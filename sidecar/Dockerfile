# python:3.12-slim
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

COPY classify.py streams.py app.py classify_rules.json ./

ENV PYTHONUNBUFFERED=1
# Single worker keeps classify LRU + _ensured cache process-local (no 4× cold ES PUTs).
ENV UVICORN_WORKERS=1
EXPOSE 8080

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port 8080 --workers ${UVICORN_WORKERS}"]
