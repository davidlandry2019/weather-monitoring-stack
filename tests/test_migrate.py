import argparse
import json
from datetime import datetime, timezone

import pytest

import clickhouse_schema as chs
import migrate_influx_to_clickhouse as mig

DAY = 86400
T0 = 1_700_000_000 - (1_700_000_000 % DAY)  # a UTC midnight


def dt(epoch, micro=0):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).replace(microsecond=micro)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_time_and_rfc3339():
    assert mig.parse_time("2024-01-02") == 1704153600
    assert mig.parse_time("2024-01-02T00:00:00Z") == 1704153600
    assert mig.parse_time("2024-01-02T01:00:00+01:00") == 1704153600
    assert mig.parse_time("1704153600") == 1704153600
    assert mig.rfc3339(1704153600) == "2024-01-02T00:00:00Z"


def test_iter_chunks():
    assert list(mig.iter_chunks(0, 250, 100)) == [(0, 100), (100, 200), (200, 250)]
    assert list(mig.iter_chunks(5, 5, 100)) == []


def test_record_to_row_handles_old_schema_and_unknowns():
    stats = mig.MappingStats()
    values = {
        "result": "_result", "table": 0, "_start": dt(0), "_stop": dt(1), "_measurement": "tempest_observations",
        "_time": dt(T0, 999_999),
        "station_id": "1", "station_name": "Backyard",
        "pressure_trend": "steady",  # tag in poller < v1.9
        "air_temperature": 70.5, "lightning_strike_count": 3,  # int field
        "mystery_metric": 1.25,  # numeric, no column -> extra
        "geocode_source": "nominatim",  # string, no column -> dropped
        "uv": None,
    }
    row = mig.record_to_row(chs.TEMPEST_OBSERVATIONS, values, stats)
    assert row["time"] == T0  # sub-second truncated
    assert row["pressure_trend"] == "steady" and row["lightning_strike_count"] == 3.0
    assert row["extra"] == {"mystery_metric": 1.25}
    assert stats.dropped == {"geocode_source": 1} and stats.to_extra == {"mystery_metric": 1}
    assert "uv" not in row and "_measurement" not in row


def test_record_to_row_owm_weather_tags_and_alert_ints():
    stats = mig.MappingStats()
    row = mig.record_to_row(chs.OWM_OBSERVATIONS, {
        "_time": dt(T0), "location": "Denver,CO,US", "weather_main": "Rain", "temp": 40.0}, stats)
    assert row["weather_main"] == "Rain" and "state" not in row  # absent tag -> '' at insert time
    alert = mig.record_to_row(chs.OWM_NWS_ALERTS, {
        "_time": dt(T0), "location": "x", "event": "e", "severity": "Minor", "active": 1, "headline": "h"}, stats)
    assert alert["active"] == 1 and "extra" not in alert
    summary = mig.record_to_row(chs.OWM_NWS_ALERT_SUMMARY, {
        "_time": dt(T0), "location": "x", "alert_count": 2, "max_severity_rank": 3.0, "bogus": 1}, stats)
    assert summary["max_severity_rank"] == 3 and stats.dropped["bogus"] == 1


def test_merge_rows_combines_fragmented_series():
    stats = mig.MappingStats()
    t = chs.TEMPEST_OBSERVATIONS
    rows = [
        {"time": T0, "station_id": "1", "station_name": "Old", "air_temperature": 50.0, "extra": {"a": 1.0}},
        {"time": T0, "station_id": "1", "station_name": "New", "pressure_trend": "rising", "extra": {"b": 2.0}},
        {"time": T0 + 60, "station_id": "1", "air_temperature": 51.0, "extra": {}},
    ]
    merged = mig.merge_rows(t, rows, stats)
    assert stats.merged == 1 and len(merged) == 2
    m = merged[("1", T0)]
    assert m["station_name"] == "New" and m["air_temperature"] == 50.0 and m["pressure_trend"] == "rising"
    assert m["extra"] == {"a": 1.0, "b": 2.0}


def test_compare_rows():
    t = chs.TEMPEST_NWS_ALERT_SUMMARY
    influx = {("1", T0): {"time": T0, "station_id": "1", "alert_count": 1, "max_severity_rank": 2},
              ("1", T0 + 300): {"time": T0 + 300, "station_id": "1", "alert_count": 0, "max_severity_rank": 0}}
    ch = {("1", T0): dict(zip(t.column_names, t.row_values(influx[("1", T0)]))),
          ("2", T0): dict(zip(t.column_names, t.row_values({"time": T0, "station_id": "2"})))}
    r = mig.compare_rows(t, influx, ch)
    assert (r["checked"], r["missing"], r["mismatched"], r["extra_in_clickhouse"]) == (2, 1, 0, 1)
    ch[("1", T0)]["alert_count"] = 9
    r = mig.compare_rows(t, influx, ch)
    assert r["mismatched"] == 1 and any("alert_count: influx=1 clickhouse=9" in e for e in r["examples"])


def test_values_equal():
    assert mig.values_equal(1.0, 1.0 + 1e-13)
    assert not mig.values_equal(1.0, None)
    assert mig.values_equal(None, None)
    assert mig.values_equal({"a": 1.0}, {"a": 1})
    assert not mig.values_equal({"a": 1.0}, {})
    assert mig.values_equal("", "")


# ---------------------------------------------------------------------------
# End to end against fakes
# ---------------------------------------------------------------------------


class FakeInflux:
    """Serves pre-pivoted records per measurement, filtered by [start, stop)."""

    bucket = "tempest"
    url = "http://influx:8086"

    def __init__(self, records):
        self.records = records  # measurement -> list of value dicts

    def time_bound(self, measurement, which):
        times = [int(r["_time"].timestamp()) for r in self.records.get(measurement, [])]
        if not times:
            return None
        return min(times) if which == "first" else max(times)

    def pivoted_records(self, measurement, start, stop):
        for r in self.records.get(measurement, []):
            if start <= r["_time"].timestamp() < stop:
                yield dict(r)


def tempest_fixture():
    obs = []
    for i in range(3 * 24 * 12):  # 3 days of 5-minute observations
        t = T0 + 3600 + i * 300
        obs.append({"_time": dt(t), "station_id": "1", "station_name": "Backyard", "location_name": "Town",
                    "air_temperature": 60.0 + i % 7, "precip": 0.01 * (i % 3), "latitude": 39.0, "longitude": -94.0,
                    "pressure_trend": "steady", "new_field": float(i)})
    alerts = [{"_time": dt(T0 + DAY + 600), "station_id": "1", "station_name": "Backyard", "location_name": "Town",
               "event": "Wind Advisory", "severity": "Moderate", "headline": "h", "urgency": "Expected",
               "expires": "2026-01-01T00:00:00Z", "active": 1}]
    summary = [{"_time": dt(T0 + DAY + 600), "station_id": "1", "station_name": "Backyard", "location_name": "Town",
                "alert_count": 1, "max_severity_rank": 2}]
    return FakeInflux({"tempest_observations": obs, "tempest_nws_alerts": alerts,
                       "tempest_nws_alert_summary": summary})


def args_for(tmp_path, **kw):
    base = dict(pipeline="tempest", database="tempest", measurements=None, start=None, stop=str(T0 + 5 * DAY),
                chunk_hours=24.0, dry_run=False, verify=True, restart=False,
                state_file=str(tmp_path / "state.json"))
    base.update(kw)
    return argparse.Namespace(**base)


def test_migrate_then_verify(tmp_path, fake_clickhouse):
    source = tempest_fixture()
    code, summary = mig.run_chunks(args_for(tmp_path), source, fake_clickhouse, write=True)
    assert code == 0, summary
    obs = fake_clickhouse.data[("tempest", "tempest_observations")]
    assert len(obs) == 3 * 24 * 12
    assert next(iter(obs.values()))["extra"] == {"new_field": 0.0}
    assert len(fake_clickhouse.data[("tempest", "tempest_nws_alerts")]) == 1
    assert "864 influx rows -> 864 clickhouse rows" in summary[0] and "0 missing, 0 mismatched" in summary[0]

    state = json.loads((tmp_path / "state.json").read_text())
    assert all(v["done_until"] == T0 + 5 * DAY for v in state.values())

    # verify mode: clean, then catches a missing and a changed row
    vargs = args_for(tmp_path, state_file=None)
    assert mig.run_chunks(vargs, source, fake_clickhouse, write=False)[0] == 0
    fake_clickhouse.delete_where("tempest", "tempest_observations", lambda r: r["time"] == T0 + 3600)
    next(iter(fake_clickhouse.data[("tempest", "tempest_observations")].values()))["air_temperature"] = -1.0
    code, summary = mig.run_chunks(vargs, source, fake_clickhouse, write=False)
    assert code == 1
    assert "1 missing, 1 mismatched" in summary[0]


def test_resume_skips_completed_chunks(tmp_path, fake_clickhouse):
    source = tempest_fixture()
    args = args_for(tmp_path, measurements=["tempest_observations"])
    assert mig.run_chunks(args, source, fake_clickhouse, write=True)[0] == 0
    n_inserts = len(fake_clickhouse.inserts)
    code, summary = mig.run_chunks(args, source, fake_clickhouse, write=True)
    assert code == 0 and summary == [] and len(fake_clickhouse.inserts) == n_inserts
    # --restart re-copies everything (idempotent thanks to the dedup key)
    code, _ = mig.run_chunks(args_for(tmp_path, measurements=["tempest_observations"], restart=True),
                             source, fake_clickhouse, write=True)
    assert code == 0 and len(fake_clickhouse.inserts) > n_inserts
    assert len(fake_clickhouse.data[("tempest", "tempest_observations")]) == 3 * 24 * 12


def test_dry_run_writes_nothing(tmp_path):
    code, summary = mig.run_chunks(args_for(tmp_path, dry_run=True), tempest_fixture(), None, write=True)
    assert code == 0 and "inserted" not in summary[0]
    assert not (tmp_path / "state.json").exists()


def test_migration_stops_on_verification_failure(tmp_path, fake_clickhouse, monkeypatch):
    real_insert = fake_clickhouse.insert

    def lossy_insert(table, data, column_names, database):
        real_insert(table, data[:-1], column_names, database)  # silently drops one row

    monkeypatch.setattr(fake_clickhouse, "insert", lossy_insert)
    code, _ = mig.run_chunks(args_for(tmp_path, measurements=["tempest_observations"]),
                             tempest_fixture(), fake_clickhouse, write=True)
    assert code == 1
    assert not (tmp_path / "state.json").exists()  # failed chunk not marked done


def test_unknown_measurement_rejected():
    with pytest.raises(SystemExit):
        mig.migratable_tables("tempest", ["owm_observations"])


def test_fetch_clickhouse_rows_sql(fake_clickhouse):
    t = chs.OWM_OBSERVATIONS
    chs.insert_rows(fake_clickhouse, "weathermap", t, [{"time": T0, "location": "Denver,CO,US", "temp": 1.0}])
    rows = mig.fetch_clickhouse_rows(fake_clickhouse, "weathermap", t, T0, T0 + 1)
    assert rows[("Denver,CO,US", T0)]["temp"] == 1.0
    assert mig.fetch_clickhouse_rows(fake_clickhouse, "weathermap", t, T0 + 1, T0 + 2) == {}
