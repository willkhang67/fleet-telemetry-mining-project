from simulator.events import SCHEMA_VERSION, make_event


def test_make_event_has_required_fields() -> None:
    event = make_event("T001")
    assert event["schema_version"] == SCHEMA_VERSION
    assert event["truck_id"] == "T001"
    assert event["event_id"]
    assert "event_time" in event
    assert 0 <= event["speed_kph"] <= 60
    assert 10 <= event["fuel_level_pct"] <= 100
    assert 70 <= event["engine_temp_c"] <= 105


def test_event_ids_are_unique() -> None:
    ids = {make_event("T001")["event_id"] for _ in range(50)}
    assert len(ids) == 50
