"""Генерация виртуальных температурных датчиков и их показателей."""

from datetime import datetime, timezone
import random
from typing import Any


LOCATIONS = ["Sector-A", "Sector-B", "Sector-C", "Sector-D"]
DEVICE_TYPE = "temperature"
UNIT = "C"


def create_devices(count: int = 20) -> list[dict[str, Any]]:
    """Создаёт count виртуальных устройств с уникальными ID.

    По умолчанию создаются 20 датчиков. Для каждого устройства задаются
    тип, местоположение, начальный статус и время последнего обновления.
    """
    if count < 1:
        raise ValueError("Количество устройств должно быть не меньше 1")

    devices: list[dict[str, Any]] = []

    for number in range(1, count + 1):
        device = {
            "device_id": f"SENSOR-{number:03d}",
            "type": DEVICE_TYPE,
            "location": LOCATIONS[(number - 1) % len(LOCATIONS)],
            "status": "active",
            "last_updated": None,
        }
        devices.append(device)

    return devices


def generate_measurement(
    device: dict[str, Any],
    anomaly_probability: float = 0.05,
) -> dict[str, Any]:
    """Генерирует один показатель для устройства.

    Обычная температура: 15–35 °C.
    Аномальная температура: выше 40 °C, в диапазоне 41–70 °C.
    По умолчанию вероятность аномалии составляет 5% для каждого измерения.
    """
    if not 0.0 <= anomaly_probability <= 1.0:
        raise ValueError("anomaly_probability должна быть в диапазоне от 0 до 1")

    if random.random() < anomaly_probability:
        value = round(random.uniform(41.0, 70.0), 1)
    else:
        value = round(random.uniform(15.0, 35.0), 1)

    measured_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )

    return {
        "device_id": device["device_id"],
        "type": device["type"],
        "location": device["location"],
        "value": value,
        "unit": UNIT,
        "status": device["status"],
        "measured_at": measured_at,
    }
