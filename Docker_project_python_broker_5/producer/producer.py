import argparse
import json
import os
import random
import time
from datetime import datetime

import pika

HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "guest")
PASSWORD = os.getenv("RABBITMQ_PASS", "guest")
QUEUE = os.getenv("QUEUE_NAME", "orders")

CUSTOMERS = ["Ivan", "Olena", "Petro", "Maria", "Andriy"]
PRODUCTS = {"Laptop": 22500, "Phone": 15000, "Mouse": 500, "Monitor": 7000}


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


def make_order(order_id, customer, product, quantity, price):
    return {
        "order_id": order_id,
        "customer": customer,
        "product": product,
        "quantity": quantity,
        "total_price": price * quantity,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


def send(channel, order):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )
    print(f"Надіслано: {order['order_id']} ({order['customer']}, "
          f"{order['product']} x{order['quantity']})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10,
                        help="кількість автоматичних замовлень")
    parser.add_argument("--manual", action="store_true",
                        help="створити замовлення вручну")
    parser.add_argument("--bad", action="store_true",
                        help="відправити одне некоректне повідомлення")
    args = parser.parse_args()

    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)

    if args.bad:
        bad_body = '{"order_id": "ORD-BAD", "customer": "Ivan", "product": '
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=bad_body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        print("Надіслано некоректне повідомлення (битий JSON)")
    elif args.manual:
        customer = input("Ім'я покупця: ")
        product = input(f"Товар {list(PRODUCTS)}: ")
        quantity = int(input("Кількість: "))
        price = PRODUCTS.get(product, 1000)
        send(channel, make_order(f"ORD-{int(time.time())}", customer,
                                 product, quantity, price))
    else:
        start = random.randint(1000, 9000)
        for i in range(args.count):
            if args.bad and i == args.count // 2:
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE,
                    body='{"order_id": "ORD-BAD", "customer": "Ivan", "product": ',
                    properties=pika.BasicProperties(delivery_mode=2,
                                                    content_type="application/json"),
                )
                print("Надіслано некоректне повідомлення (битий JSON)")
                continue
            product = random.choice(list(PRODUCTS))
            order = make_order(
                f"ORD-{start + i}",
                random.choice(CUSTOMERS),
                product,
                random.randint(1, 3),
                PRODUCTS[product],
            )
            send(channel, order)

    connection.close()


if __name__ == "__main__":
    main()