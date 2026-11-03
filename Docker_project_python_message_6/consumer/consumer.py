import asyncio
import json
import os
import random
import signal
import socket

import aio_pika

HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "guest")
PASSWORD = os.getenv("RABBITMQ_PASS", "guest")
QUEUE = os.getenv("QUEUE_NAME", "support_requests")
PREFETCH = int(os.getenv("PREFETCH_COUNT", "1"))

WORKER_NAME = os.getenv("WORKER_NAME", socket.gethostname())
REQUIRED = ["id", "user", "category", "message", "created_at"]

DEPARTMENTS = {
    "technical": "Звернення передано технічному спеціалісту",
    "payment": "Звернення передано фінансовому відділу",
    "delivery": "Звернення передано відділу доставки",
    "other": "Звернення передано загальній підтримці",
}

stop_event = asyncio.Event()
busy = False


def validate(request):
    if not isinstance(request, dict):
        raise ValueError("повідомлення має бути JSON-об'єктом")
    for field in REQUIRED:
        if field not in request:
            raise ValueError(f"немає поля {field}")
    if request["category"] not in DEPARTMENTS:
        raise ValueError(f"невідома категорія {request['category']}")


async def connect():
    for attempt in range(10):
        try:
            return await aio_pika.connect_robust(
                host=HOST, port=PORT, login=USER, password=PASSWORD,
            )
        except Exception as e:
            print(f"[{WORKER_NAME}] RabbitMQ ще не готовий ({e}), спроба {attempt + 1}/10...")
            await asyncio.sleep(3)
    raise SystemExit("Не вдалося підключитися до RabbitMQ")


async def handle(message: aio_pika.IncomingMessage):
    global busy
    busy = True
    try:
        try:
            request = json.loads(message.body)
            validate(request)
        except (ValueError, TypeError) as e:
            print(f"[{WORKER_NAME}] Некоректне звернення: {e}")
            await message.reject(requeue=False)
            return

        req_id = request["id"]
        category = request["category"]

        try:
            print(f"[{WORKER_NAME}] Received request #{req_id} | "
                  f"{request['user']} | {category}: {request['message']}")
            await asyncio.sleep(random.uniform(2, 5))
            print(f"[{WORKER_NAME}] {DEPARTMENTS[category]} (#{req_id})")
            await message.ack()
            print(f"[{WORKER_NAME}] Finished request #{req_id}")
        except Exception as e:
            print(f"[{WORKER_NAME}] Помилка обробки #{req_id}: {e}")
            await message.nack(requeue=True)
    finally:
        busy = False


async def consume(queue: aio_pika.abc.AbstractQueue):
    async with queue.iterator() as messages:
        async for message in messages:
            await handle(message)
            if stop_event.is_set():
                break


async def main():
    loop = asyncio.get_running_loop()

    connection = await connect()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH)
    queue = await channel.declare_queue(QUEUE, durable=True)
    print(f"[{WORKER_NAME}] Worker запущено, чекаю на звернення...")

    consumer_task = asyncio.create_task(consume(queue))

    def on_stop():
        print(f"[{WORKER_NAME}] Отримано сигнал зупинки...")
        stop_event.set()
        if not busy:
            consumer_task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, on_stop)

    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await connection.close()
    print(f"[{WORKER_NAME}] Worker зупинено коректно")


if __name__ == "__main__":
    asyncio.run(main())