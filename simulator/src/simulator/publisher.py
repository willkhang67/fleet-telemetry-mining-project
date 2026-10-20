"""Kafka producer wrapper for telemetry events."""

from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class TelemetryPublisher:
    """Publishes JSON telemetry events with delivery accounting."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err: object, _msg: object) -> None:
        if err is not None:
            self.failed += 1
            logger.error("delivery failed: %s", err)
        else:
            self.delivered += 1

    def publish(self, key: str, event: dict[str, Any]) -> None:
        self._producer.produce(
            self._topic,
            key=key,
            value=json.dumps(event, separators=(",", ":")),
            callback=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout_sec: float) -> int:
        """Flush outstanding messages. Returns count still unsent."""
        return int(self._producer.flush(timeout_sec))
