FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
ENV PIP_REQUIRE_HASHES=0
RUN pip install --no-cache-dir -r requirements.txt

COPY classify.py streams.py app.py ./

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "4"]
