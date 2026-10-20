"""Synthetic telemetry event generation.

All values are placeholders for pipeline testing. They are not derived from
real mining operations or proprietary fleet data.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any

# Assumptions (plausible placeholders only — document in README as well):
# - Position: ~Pilbara-ish centroid (-22.5, 118.9) ± 0.05°
# - Speed: 0–60 kph
# - Fuel: 10–100%
# - Engine temperature: 70–105 °C
# - Fields are independent (no vehicle physics model)
SCHEMA_VERSION = 1
BASE_LATITUDE = -22.5
BASE_LONGITUDE = 118.9
POSITION_JITTER_DEG = 0.05
SPEED_KPH_RANGE = (0.0, 60.0)
FUEL_PCT_RANGE = (10.0, 100.0)
ENGINE_TEMP_C_RANGE = (70.0, 105.0)


def make_event(truck_id: str, *, rng: random.Random | None = None) -> dict[str, Any]:
    """Build one synthetic telemetry event for ``truck_id``."""
    r = rng or random

    return {
        "schema_version": SCHEMA_VERSION,
        "truck_id": truck_id,
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "latitude": BASE_LATITUDE + r.uniform(-POSITION_JITTER_DEG, POSITION_JITTER_DEG),
        "longitude": BASE_LONGITUDE + r.uniform(-POSITION_JITTER_DEG, POSITION_JITTER_DEG),
        "speed_kph": round(r.uniform(*SPEED_KPH_RANGE), 1),
        "fuel_level_pct": round(r.uniform(*FUEL_PCT_RANGE), 1),
        "engine_temp_c": round(r.uniform(*ENGINE_TEMP_C_RANGE), 1),
    }
