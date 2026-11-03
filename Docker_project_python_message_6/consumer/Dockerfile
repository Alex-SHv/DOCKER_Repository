FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir aio-pika==9.4.3
COPY consumer.py .
CMD ["python", "-u", "consumer.py"]