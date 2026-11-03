import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime

import pika

HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "guest")
PASSWORD = os.getenv("RABBITMQ_PASS", "guest")
QUEUE = os.getenv("QUEUE_NAME", "support_requests")

CATEGORIES = {
    "technical": "технічна проблема",
    "payment": "питання оплати",
    "delivery": "питання доставки",
    "other": "інші питання",
}

SAMPLE_USERS = ["Ivan", "Olena", "Petro", "Maria", "Andriy"]
SAMPLE_MESSAGES = {
    "technical": ["The site does not load", "The app crashes at checkout"],
    "payment": ["I cannot complete my payment", "The card was charged twice"],
    "delivery": ["My order has not arrived yet", "I want to change the delivery address"],
    "other": ["How do I return an item?", "Do you have a loyalty program?"],
}


def connect():
    params = pika.ConnectionParameters(
        host=HOST, port=PORT,
        credentials=pika.PlainCredentials(USER, PASSWORD),
    )
    for attempt in range(10):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print(f"RabbitMQ ще не готовий, спроба {attempt + 1}/10...")
            time.sleep(3)
    raise SystemExit("Не вдалося підключитися до RabbitMQ")


def make_request(user, category, message):
    return {
        "id": uuid.uuid4().int % 10**9,
        "user": user,
        "category": category,
        "message": message,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def publish(channel, request):
    try:
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=json.dumps(request, ensure_ascii=False),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
    except (pika.exceptions.AMQPError, pika.exceptions.MessageError) as e:
        print(f"Звернення #{request['id']} НЕ прийнято: {e}")
        return False

    print(f"Звернення #{request['id']} успішно прийнято")
    return True

def ask_interactive(channel):
    user = input("\nІм'я користувача (q — вихід): ").strip()
    if user.lower() == "q":
        return False
    if not user:
        print("Ім'я не може бути порожнім")
        return True

    print("Категорії:")
    for key, title in CATEGORIES.items():
        print(f"  {key} — {title}")
    category = input("Категорія: ").strip().lower()
    if category not in CATEGORIES:
        print("Невідома категорія")
        return True

    message = input("Текст звернення: ").strip()
    if not message:
        print("Текст звернення не може бути порожнім")
        return True

    publish(channel, make_request(user, category, message))
    return True


def run_auto(channel, count):
    accepted = 0
    for _ in range(count):
        category = random.choice(list(CATEGORIES))
        request = make_request(
            random.choice(SAMPLE_USERS),
            category,
            random.choice(SAMPLE_MESSAGES[category]),
        )
        if publish(channel, request):
            accepted += 1
    print(f"Прийнято брокером: {accepted} з {count}")


def main():
    parser = argparse.ArgumentParser(description="Support Client")
    parser.add_argument("--auto", type=int, default=0,
                        help="надіслати N випадкових звернень без введення")
    args = parser.parse_args()

    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.confirm_delivery()

    try:
        if args.auto:
            run_auto(channel, args.auto)
        else:
            print("Служба підтримки: створення звернень. Введіть q, щоб вийти.")
            while ask_interactive(channel):
                pass
    except (KeyboardInterrupt, EOFError):
        print("\nВихід")
    finally:
        connection.close()


if __name__ == "__main__":
    main()