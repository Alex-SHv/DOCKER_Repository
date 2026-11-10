import json
import math
import os
import random
import signal
import socket
import sys
import time

import pika

HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "admin")
PASSWORD = os.getenv("RABBITMQ_PASS", "admin")
EXCHANGE = os.getenv("EXCHANGE", "factory")
PREFETCH = int(os.getenv("PREFETCH_COUNT", "1"))
PROCESS_DELAY = float(os.getenv("PROCESS_DELAY", "0"))
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

QUEUE = "industrial_sensors"
WORKER_ID = socket.gethostname()
REQUIRED = ["sensor_id", "type", "value", "timestamp"]

THRESHOLDS = {
    "temperature": {"unit": "°C", "warn": 70.0, "crit": 90.0},
    "pressure": {"unit": "bar", "warn": 8.0, "crit": 10.0},
    "vibration": {"unit": "mm/s", "warn": 4.0, "crit": 7.0},
    "humidity": {"unit": "%", "warn": 60.0, "crit": 80.0},
}
PREFIXES = {"NORMAL": "[INFO]", "WARNING": "[WARNING]", "CRITICAL": "[CRITICAL]"}

stop = False
stats = {"NORMAL": 0, "WARNING": 0, "CRITICAL": 0, "INVALID": 0, "RETRIED": 0, "FAILED": 0}


class InvalidMessage(Exception):
    """Повідомлення некоректне: повтор не допоможе."""


def handle_stop(signum, frame):
    global stop
    print(f"[worker {WORKER_ID}] Отримано сигнал зупинки, завершую поточне повідомлення...")
    stop = True


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


def parse_reading(body):
    try:
        reading = json.loads(body)
    except ValueError as e:
        raise InvalidMessage(f"битий JSON ({e})")
    if not isinstance(reading, dict):
        raise InvalidMessage("повідомлення не є JSON-об'єктом")
    for field in REQUIRED:
        if field not in reading:
            raise InvalidMessage(f"немає поля {field}")
    value = reading["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise InvalidMessage("value має бути числом")
    if reading["type"] not in THRESHOLDS:
        raise InvalidMessage(f"невідомий тип датчика {reading['type']}")
    return reading


def classify(sensor_type, value):
    limits = THRESHOLDS[sensor_type]
    if value > limits["crit"]:
        return "CRITICAL"
    if value > limits["warn"]:
        return "WARNING"
    return "NORMAL"


def publish_alert(channel, reading):
    limits = THRESHOLDS[reading["type"]]
    alert = {
        "sensor_id": reading["sensor_id"],
        "type": reading["type"],
        "value": reading["value"],
        "unit": limits["unit"],
        "threshold": limits["crit"],
        "timestamp": reading["timestamp"],
        "message": "Терміново перевірте обладнання",
    }
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key="alert.critical",
        body=json.dumps(alert, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )


def analyze(channel, reading):
    if PROCESS_DELAY:
        time.sleep(PROCESS_DELAY)
    if FAILURE_RATE and random.random() < FAILURE_RATE:
        raise RuntimeError("тимчасовий збій (імітація)")
    state = classify(reading["type"], reading["value"])
    if state == "CRITICAL":
        publish_alert(channel, reading)
    return state


def on_message(channel, method, props, body):
    try:
        reading = parse_reading(body)
    except InvalidMessage as e:
        stats["INVALID"] += 1
        print(f"[worker {WORKER_ID}] Некоректне повідомлення: {e}")
        channel.basic_reject(method.delivery_tag, requeue=False)
        return

    attempt = int((props.headers or {}).get("x-attempts", 0)) + 1
    try:
        state = analyze(channel, reading)
    except Exception as e:
        if attempt < MAX_ATTEMPTS:
            stats["RETRIED"] += 1
            print(f"[worker {WORKER_ID}] Помилка обробки: {e}. Повторна спроба {attempt + 1}/{MAX_ATTEMPTS}")
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=method.routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    headers={"x-attempts": attempt},
                ),
            )
            channel.basic_ack(method.delivery_tag)
        else:
            stats["FAILED"] += 1
            print(f"[worker {WORKER_ID}] Відхилено після {attempt} спроб: {e}")
            channel.basic_reject(method.delivery_tag, requeue=False)
        return

    stats[state] += 1
    unit = THRESHOLDS[reading["type"]]["unit"]
    print(f"{PREFIXES[state]} {reading['sensor_id']}: {reading['value']:.1f} {unit} — {state}")
    if state == "CRITICAL":
        print("    аварію передано в critical_alerts: потрібна термінова перевірка")

    channel.basic_ack(method.delivery_tag)


def print_summary():
    print(
        f"[worker {WORKER_ID}] Підсумок: норма={stats['NORMAL']}, попередження={stats['WARNING']}, "
        f"аварії={stats['CRITICAL']}, некоректних={stats['INVALID']}, "
        f"повторних спроб={stats['RETRIED']}, відхилено={stats['FAILED']}"
    )


def main():
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    connection = connect()
    channel = connection.channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=PREFETCH)
    print(f"[worker {WORKER_ID}] Worker запущено, чекаю на показники...")

    for method, props, body in channel.consume(QUEUE, inactivity_timeout=1):
        if stop:
            break
        if method is None:
            continue
        on_message(channel, method, props, body)

    channel.cancel()
    connection.close()
    print_summary()
    print(f"[worker {WORKER_ID}] Worker зупинено коректно")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    main()