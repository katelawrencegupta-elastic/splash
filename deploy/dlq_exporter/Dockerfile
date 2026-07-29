FROM python:3.12-slim
WORKDIR /app
COPY dlq_exporter.py .
EXPOSE 9102
CMD ["python", "dlq_exporter.py"]
