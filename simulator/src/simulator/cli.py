"""CLI entrypoint for the fleet telemetry simulator."""

from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
from dataclasses import replace

from simulator.config import Settings
from simulator.events import make_event
from simulator.publisher import TelemetryPublisher

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simulator",
        description=(
            "Publish synthetic mining-fleet telemetry to Kafka. "
            "Data is simulated — not real mine telemetry."
        ),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Kafka bootstrap servers (default: env KAFKA_BOOTSTRAP_SERVERS or localhost:9094)",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Kafka topic (default: env KAFKA_TOPIC or telemetry)",
    )
    parser.add_argument(
        "--trucks",
        type=int,
        default=None,
        help="Number of trucks to simulate (default: 10)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Send exactly N events then exit (smoke mode; default 100 if no rate/duration)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        metavar="EVENTS_PER_SEC",
        help="Sustained publish rate (events/sec). Use with --duration or until Ctrl+C",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Stop after this many seconds (rate mode)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def resolve_settings(args: argparse.Namespace) -> Settings:
    base = Settings.from_env()
    updates: dict[str, object] = {}

    if args.bootstrap_servers is not None:
        updates["bootstrap_servers"] = args.bootstrap_servers
    if args.topic is not None:
        updates["topic"] = args.topic
    if args.trucks is not None:
        updates["truck_count"] = args.trucks
    if args.count is not None:
        updates["count"] = args.count
        updates["rate_per_sec"] = None
        updates["duration_sec"] = None
    if args.rate is not None:
        updates["rate_per_sec"] = args.rate
        if args.count is None:
            updates["count"] = None
    if args.duration is not None:
        updates["duration_sec"] = args.duration
        if args.count is None and args.rate is None and base.rate_per_sec is None:
            updates["rate_per_sec"] = 10.0
            updates["count"] = None

    return replace(base, **updates) if updates else base


def run(settings: Settings) -> int:
    trucks = settings.truck_ids()
    publisher = TelemetryPublisher(settings.bootstrap_servers, settings.topic)
    stop = False

    def _handle_signal(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True
        logger.info("shutdown requested, flushing…")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "starting publisher bootstrap=%s topic=%s trucks=%d count=%s rate=%s duration=%s",
        settings.bootstrap_servers,
        settings.topic,
        settings.truck_count,
        settings.count,
        settings.rate_per_sec,
        settings.duration_sec,
    )

    sent = 0
    started = time.monotonic()
    next_deadline = started

    try:
        while not stop:
            if settings.count is not None and sent >= settings.count:
                break
            if settings.duration_sec is not None and (time.monotonic() - started) >= settings.duration_sec:
                break

            if settings.rate_per_sec is not None and settings.rate_per_sec > 0:
                now = time.monotonic()
                if now < next_deadline:
                    time.sleep(min(next_deadline - now, 0.05))
                next_deadline += 1.0 / settings.rate_per_sec

            truck = random.choice(trucks)
            publisher.publish(truck, make_event(truck))
            sent += 1

            if sent % 1000 == 0:
                logger.info(
                    "progress sent=%d delivered=%d failed=%d",
                    sent,
                    publisher.delivered,
                    publisher.failed,
                )
    finally:
        unsent = publisher.flush(settings.flush_timeout_sec)
        logger.info(
            "done sent=%d delivered=%d failed=%d unsent=%d elapsed_sec=%.2f",
            sent,
            publisher.delivered,
            publisher.failed,
            unsent,
            time.monotonic() - started,
        )

    return 1 if publisher.failed or unsent else 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = resolve_settings(args)
    if settings.count is None and settings.rate_per_sec is None:
        parser.error("specify --count and/or --rate (or set SIM_COUNT / SIM_RATE)")
    return run(settings)


if __name__ == "__main__":
    sys.exit(main())
