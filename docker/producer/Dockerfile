FROM python:3.12-slim

RUN pip install --no-cache-dir kafka-python

WORKDIR /app
COPY docker/producer/producer.py /app/producer.py

ENTRYPOINT ["python", "/app/producer.py"]
