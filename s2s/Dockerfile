FROM python:3.12-slim

WORKDIR /app

ENV PIP_REQUIRE_HASHES=0
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY s2s ./s2s
COPY server.py ./

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
EXPOSE 39998 8081

CMD ["python", "server.py"]
