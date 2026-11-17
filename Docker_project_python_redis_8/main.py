import redis

try:
    client = redis.Redis(host='localhost', port=6379, decode_responses=True, protocol=2)

    if client.ping():
        print("Успешное подключение к Redis!")

    client.set('greeting', 'Привет из Python!')
    print("Данные успешно записаны в Redis.")

    value = client.get('greeting')
    print(f"Получено из Redis: {value}")

except redis.ConnectionError:
    print("Ошибка: Не удалось подключиться к Redis.")
    print("Убедитесь, что сервер redis-server запущен и доступен на порту 6379.")