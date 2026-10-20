"""Runtime configuration for the telemetry simulator."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


@dataclass(frozen=True, slots=True)
class Settings:
    """Kafka and load-generation settings.

    Defaults match local Docker Compose (EXTERNAL listener on localhost:9094).
    Override via environment variables or CLI flags.
    """

    bootstrap_servers: str = "localhost:9094"
    topic: str = "telemetry"
    truck_count: int = 10
    count: int | None = 100
    rate_per_sec: float | None = None
    duration_sec: float | None = None
    flush_timeout_sec: float = 10.0

    @classmethod
    def from_env(cls) -> Settings:
        count_raw = os.getenv("SIM_COUNT")
        rate_raw = os.getenv("SIM_RATE")
        duration_raw = os.getenv("SIM_DURATION_SEC")

        count: int | None
        if count_raw is not None and count_raw != "":
            count = int(count_raw)
        elif rate_raw or duration_raw:
            count = None
        else:
            count = 100

        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
            topic=os.getenv("KAFKA_TOPIC", "telemetry"),
            truck_count=_env_int("SIM_TRUCK_COUNT", 10),
            count=count,
            rate_per_sec=_env_float("SIM_RATE", 0.0) or None,
            duration_sec=_env_float("SIM_DURATION_SEC", 0.0) or None,
            flush_timeout_sec=_env_float("SIM_FLUSH_TIMEOUT_SEC", 10.0),
        )

    def truck_ids(self) -> list[str]:
        return [f"T{n:03d}" for n in range(1, self.truck_count + 1)]
