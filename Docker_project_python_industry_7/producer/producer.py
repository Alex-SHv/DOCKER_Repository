import argparse
import json
import os
import random
import sys
import time
from datetime import datetime

import pika

HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "admin")
PASSWORD = os.getenv("RABBITMQ_PASS", "admin")
EXCHANGE = os.getenv("EXCHANGE", "factory")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "1"))
ACTIVE = [s.strip() for s in os.getenv("SENSORS", "TEMP-01,PRESS-01,VIB-01").split(",") if s.strip()]

SENSORS = {
    "TEMP-01": {"type": "temperature", "low": 50.0, "high": 100.0},
    "PRESS-01": {"type": "pressure", "low": 5.0, "high": 12.0},
    "VIB-01": {"type": "vibration", "low": 1.0, "high": 9.0},
    "HUM-01": {"type": "humidity", "low": 40.0, "high": 95.0},
}


def describe_error(e):
    detail = str(e) or ", ".join(repr(a) for a in e.args)
    return f"{type(e).__name__}: {detail}" if detail else type(e).__name__


def connect():
    params = pika.ConnectionParameters(
        host=HOST, port=PORT,
        credentials=pika.PlainCredentials(USER, PASSWORD),
        heartbeat=600,
    )
    last_error = "невідома помилка"
    for attempt in range(10):
        try:
            return pika.BlockingConnection(params)
        except (pika.exceptions.AMQPConnectionError, OSError) as e:
            last_error = describe_error(e)
            print(f"RabbitMQ ще не готовий ({HOST}:{PORT}), спроба {attempt + 1}/10: {last_error}")
            time.sleep(3)
    raise SystemExit(
        f"Не вдалося підключитися до RabbitMQ ({HOST}:{PORT}): {last_error}\n"
        "Перевірте: брокер запущено (docker compose up -d rabbitmq), для запуску з PyCharm "
        "RABBITMQ_HOST = localhost, логін і пароль за замовчуванням admin/admin."
    )


def declare_topology(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue="industrial_sensors", durable=True)
    channel.queue_bind(queue="industrial_sensors", exchange=EXCHANGE, routing_key="sensor.#")
    channel.queue_declare(queue="critical_alerts", durable=True)
    channel.queue_bind(queue="critical_alerts", exchange=EXCHANGE, routing_key="alert.critical")


def make_reading(sensor_id, value=None):
    sensor = SENSORS[sensor_id]
    if value is None:
        value = random.uniform(sensor["low"], sensor["high"])
    return {
        "sensor_id": sensor_id,
        "type": sensor["type"],
        "value": round(value, 1),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def send(channel, reading):
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=f"sensor.{reading['type']}",
        body=json.dumps(reading),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )
    print(f"Надіслано: {reading['sensor_id']} = {reading['value']} ({reading['type']})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=int(os.getenv("MESSAGE_COUNT", "0")), help="кількість показників (0 = без обмеження)")
    parser.add_argument("--manual", action="store_true", help="ввести показник вручну (для перевірки порогів)")
    parser.add_argument("--bad", action="store_true", help="відправити одне некоректне повідомлення")
    args = parser.parse_args()

    if not ACTIVE or any(s not in SENSORS for s in ACTIVE):
        raise SystemExit(f"Невідомий датчик у SENSORS. Доступні: {', '.join(SENSORS)}")

    connection = connect()
    channel = connection.channel()
    declare_topology(channel)

    if args.bad:
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key="sensor.temperature",
            body='{"sensor_id": "TEMP-01", "type": "temperature", "value": ',
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
        )
        print("Надіслано некоректне повідомлення (битий JSON)")
    elif args.manual:
        print(f"Доступні датчики: {', '.join(SENSORS)}")
        sensor_id = input("ID датчика: ").strip().upper()
        if sensor_id not in SENSORS:
            raise SystemExit(f"Невідомий датчик: {sensor_id}")
        value = float(input("Значення: ").replace(",", "."))
        send(channel, make_reading(sensor_id, value))
    else:
        sent = 0
        while args.count == 0 or sent < args.count:
            send(channel, make_reading(ACTIVE[sent % len(ACTIVE)]))
            sent += 1
            if args.count == 0 or sent < args.count:
                time.sleep(SEND_INTERVAL)

    connection.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    main()