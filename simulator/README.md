# Fleet telemetry simulator

Publishes **synthetic** mining-fleet telemetry to Kafka for local pipeline development.
This is not real mine or OEM data.

## Data assumptions

| Field | Placeholder rule |
|-------|------------------|
| Position | ~Pilbara-ish `(-22.5, 118.9)` ± `0.05°` |
| Speed | `0–60` kph |
| Fuel | `10–100` % |
| Engine temp | `70–105` °C |
| Independence | Fields are independent (no vehicle physics) |

## Prerequisites

- Kafka reachable (Docker Compose in repo root): `localhost:9094`
- Topic `telemetry` created by `kafka-init`

## Run

```bash
cd simulator
uv sync

# Smoke: 100 events (default)
uv run simulator

# Or via module / thin main.py
uv run python -m simulator
uv run python main.py

# Sustained rate until Ctrl+C
uv run simulator --rate 50

# Rate for a fixed window
uv run simulator --rate 100 --duration 30 --trucks 20

# Overrides
uv run simulator --bootstrap-servers localhost:9094 --topic telemetry --count 500 -v
```

Environment variables (optional): `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`,
`SIM_TRUCK_COUNT`, `SIM_COUNT`, `SIM_RATE`, `SIM_DURATION_SEC`.

## Layout

```
src/simulator/
  config.py      # settings from env / CLI
  events.py      # synthetic event schema + generator
  publisher.py   # Kafka produce + delivery counters
  cli.py         # argparse entrypoint
```
