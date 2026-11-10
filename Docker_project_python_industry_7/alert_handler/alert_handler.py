import json
import os
import signal
import sys
import time

import pika

HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "admin")
PASSWORD = os.getenv("RABBITMQ_PASS", "admin")
EXCHANGE = os.getenv("EXCHANGE", "factory")
ALERT_FILE = os.getenv(
    "ALERT_FILE",
    os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "alerts.log")),
)

QUEUE = "critical_alerts"
stop = False


def handle_stop(signum, frame):
    global stop
    print("[alert-handler] Отримано сигнал зупинки, завершую...")
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


def save_to_file(alert):
    try:
        folder = os.path.dirname(ALERT_FILE)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(ALERT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(alert, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"[alert-handler] Не вдалося записати аварію у файл: {e}")


def on_alert(channel, method, props, body):
    try:
        alert = json.loads(body)
        line = (f"[URGENT] {alert['sensor_id']}: {alert['value']:.1f} {alert['unit']} "
                f"— терміново перевірте обладнання")
    except (ValueError, KeyError, TypeError) as e:
        print(f"[alert-handler] Некоректна аварія: {e}")
        channel.basic_reject(method.delivery_tag, requeue=False)
        return
    print(line)
    save_to_file(alert)
    channel.basic_ack(method.delivery_tag)


def main():
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    connection = connect()
    channel = connection.channel()
    declare_topology(channel)
    channel.basic_qos(prefetch_count=1)
    print("[alert-handler] Чекаю на аварійні повідомлення...")

    for method, props, body in channel.consume(QUEUE, inactivity_timeout=1):
        if stop:
            break
        if method is None:
            continue
        on_alert(channel, method, props, body)

    channel.cancel()
    connection.close()
    print("[alert-handler] Зупинено коректно")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    main()