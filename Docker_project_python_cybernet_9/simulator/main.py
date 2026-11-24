"""Главная программа этапа 1: запуск виртуальных устройств."""

import argparse
import json
import time

from .generator import create_devices, generate_measurement


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CyberNet Device Simulator — генератор температурных датчиков"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=20,
        help="Количество виртуальных устройств (по умолчанию: 20)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Пауза между циклами измерений в секундах (по умолчанию: 5)",
    )
    parser.add_argument(
        "--anomaly-probability",
        type=float,
        default=0.05,
        help="Вероятность аномалии от 0 до 1 (по умолчанию: 0.05 = 5%%)",
    )
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count должен быть не меньше 1")
    if args.interval <= 0:
        parser.error("--interval должен быть больше 0")
    if not 0.0 <= args.anomaly_probability <= 1.0:
        parser.error("--anomaly-probability должна быть от 0 до 1")

    return args


def main() -> None:
    args = parse_args()
    devices = create_devices(args.count)

    print("CyberNet Device Simulator запущен")
    print(f"Устройств: {len(devices)}")
    print(f"Интервал между циклами: {args.interval} сек.")
    print(f"Вероятность аномалии: {args.anomaly_probability:.0%}")
    print("Для остановки нажмите Ctrl+C.\n")

    cycle = 0

    try:
        while True:
            cycle += 1
            print(f"--- Цикл измерений № {cycle} ---")

            for device in devices:
                measurement = generate_measurement(
                    device,
                    anomaly_probability=args.anomaly_probability,
                )

                # Сохраняем время последнего обновления в объекте устройства.
                device["last_updated"] = measurement["measured_at"]

                # На данном этапе данные выводятся в консоль.
                # На этапе 2 эта же структура будет отправляться в FastAPI.
                label = "АНОМАЛИЯ" if measurement["value"] > 40 else "НОРМА"
                print(
                    f"[{label}] "
                    f"{json.dumps(measurement, ensure_ascii=False)}"
                )

            print()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nСимулятор остановлен пользователем.")


if __name__ == "__main__":
    main()
