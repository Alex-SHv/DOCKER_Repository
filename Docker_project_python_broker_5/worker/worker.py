import json
import os
import random
import signal
import socket
import time

import pika

HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "guest")
PASSWORD = os.getenv("RABBITMQ_PASS", "guest")
QUEUE = os.getenv("QUEUE_NAME", "orders")
PREFETCH = int(os.getenv("PREFETCH_COUNT", "1"))

WORKER_ID = socket.gethostname()
REQUIRED = ["order_id", "customer", "product", "quantity", "total_price", "created_at"]

stop = False


def handle_stop(signum, frame):
    global stop
    print(f"[{WORKER_ID}] Отримано сигнал зупинки, завершую після поточного замовлення...")
    stop = True


def connect():
    params = pika.ConnectionParameters(
        host=HOST, port=PORT,
        credentials=pika.PlainCredentials(USER, PASSWORD),
        heartbeat=600,
    )
    for attempt in range(10):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print(f"RabbitMQ ще не готовий, спроба {attempt + 1}/10...")
            time.sleep(3)
    raise SystemExit("Не вдалося підключитися до RabbitMQ")


def validate(order):
    for field in REQUIRED:
        if field not in order:
            raise ValueError(f"немає поля {field}")
    if not isinstance(order["quantity"], int) or order["quantity"] <= 0:
        raise ValueError("quantity має бути додатним цілим")
    if order["total_price"] < 0:
        raise ValueError("total_price не може бути від'ємним")


def main():
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH)
    print(f"[{WORKER_ID}] Worker запущено, чекаю на замовлення...")

    for method, props, body in channel.consume(QUEUE, inactivity_timeout=1):
        if stop:
            break
        if method is None:
            continue

        try:
            order = json.loads(body)
            validate(order)
        except (ValueError, TypeError) as e:
            print(f"[{WORKER_ID}] Некоректне замовлення: {e}")
            channel.basic_reject(method.delivery_tag, requeue=False)
            continue

        try:
            print(f"[{WORKER_ID}] Обробляю {order['order_id']}: {order['customer']}, "
                  f"{order['product']} x{order['quantity']}, {order['total_price']} грн")
            time.sleep(random.uniform(2, 5))
            channel.basic_ack(method.delivery_tag)
            print(f"[{WORKER_ID}] Готово {order['order_id']}")
        except Exception as e:
            print(f"[{WORKER_ID}] Помилка обробки: {e}")
            channel.basic_nack(method.delivery_tag, requeue=True)

    channel.cancel()
    connection.close()
    print(f"[{WORKER_ID}] Worker зупинено коректно")


if __name__ == "__main__":
    main()