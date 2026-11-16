import pytest

import clickhouse_schema as chs
import owm_poller
import tempest_poller

# ---------------------------------------------------------------------------
# clickhouse_schema
# ---------------------------------------------------------------------------


def test_ddl_shape():
    ddl = chs.TEMPEST_OBSERVATIONS.ddl("tempest")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `tempest`.`tempest_observations`")
    assert "ENGINE = ReplacingMergeTree(inserted_at)" in ddl
    assert "ORDER BY (`station_id`, `time`)" in ddl
    assert "`inserted_at` DateTime64(3, 'UTC') DEFAULT now64(3)" in ddl
    hc = chs.POLLER_HEALTHCHECK.ddl("weathermap")
    assert "ENGINE = MergeTree" in hc and "TTL time + INTERVAL 30 DAY" in hc and "inserted_at" not in hc


def test_schema_sql_one_shared_database_has_no_name_clashes():
    names = [t.name for p in chs.PIPELINES.values() for t in p["tables"] if t.influx_measurement]
    assert len(names) == len(set(names))
    stmts = chs.schema_sql("tempest", "weather") + chs.schema_sql("owm", "weather")
    assert all("`weather`." in s or s.startswith("CREATE DATABASE") for s in stmts)


@pytest.mark.parametrize("bad", ["", "1abc", "tempest; DROP TABLE x", "a-b", "db.table", "`x`"])
def test_validate_ident_rejects(bad):
    with pytest.raises(ValueError):
        chs.validate_ident(bad)


def test_row_values_fills_empties_and_requires_time():
    vals = chs.TEMPEST_OBSERVATIONS.row_values({"time": 100, "station_id": "1", "air_temperature": 0.0})
    row = dict(zip(chs.TEMPEST_OBSERVATIONS.column_names, vals))
    assert row["air_temperature"] == 0.0  # zero is a value, not "missing"
    assert row["station_name"] == "" and row["dew_point"] is None and row["extra"] == {}
    assert row["pressure_trend"] == ""
    with pytest.raises(ValueError):
        chs.TEMPEST_OBSERVATIONS.row_values({"station_id": "1"})


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SECURE", "true")
    monkeypatch.delenv("CLICKHOUSE_PORT", raising=False)
    s = chs.clickhouse_settings_from_env()
    assert s["secure"] is True and s["port"] == 8443


def test_schema_cli_print(capsys):
    assert chs.main(["print", "--pipeline", "owm", "--database", "wx"]) == 0
    out = capsys.readouterr().out
    assert "CREATE DATABASE IF NOT EXISTS `wx`" in out and "`wx`.`owm_nws_alert_summary`" in out


# ---------------------------------------------------------------------------
# Tempest poller
# ---------------------------------------------------------------------------

TEMPEST_RESPONSE = {
    "station_id": 123456,
    "station_name": "Backyard",
    "latitude": 39.1,
    "longitude": -94.5,
    "obs": [
        {
            "timestamp": 1760000000,
            "air_temperature": 20.0,
            "wind_avg": 10.0,
            "barometric_pressure": 1000.0,
            "precip": 25.4,
            "relative_humidity": 55,
            "pressure_trend": "rising",
            "brand_new_metric": 7,
            "some_text": "abc",
            "uv": None,
        },
        "garbage",
        {"air_temperature": 1.0},  # no timestamp
    ],
}


def test_tempest_build_rows_imperial(monkeypatch):
    monkeypatch.setattr(tempest_poller, "TEMPEST_UNITS", "imperial")
    monkeypatch.setattr(tempest_poller, "STATION_LOCATION_OVERRIDES", {"123456": "Springfield, IL"})
    rows = tempest_poller.build_rows(TEMPEST_RESPONSE, "999")
    assert len(rows) == 1
    r = rows[0]
    assert r["time"] == 1760000000 and r["station_id"] == "123456"
    assert r["station_name"] == "Backyard" and r["location_name"] == "Springfield, IL"
    assert r["air_temperature"] == pytest.approx(68.0)
    assert r["wind_avg"] == pytest.approx(22.36936)
    assert r["barometric_pressure"] == pytest.approx(29.53)
    assert r["precip"] == pytest.approx(1.0)
    assert r["relative_humidity"] == 55.0
    assert r["pressure_trend"] == "rising"
    assert r["extra"] == {"brand_new_metric": 7.0}
    assert "uv" not in r and "some_text" not in r
    chs.TEMPEST_OBSERVATIONS.row_values(r)  # insertable


def test_tempest_build_rows_metric_and_no_location(monkeypatch):
    monkeypatch.setattr(tempest_poller, "TEMPEST_UNITS", "metric")
    monkeypatch.setattr(tempest_poller, "STATION_LOCATION_OVERRIDES", {})
    r = tempest_poller.build_rows(TEMPEST_RESPONSE, "999")[0]
    assert r["air_temperature"] == 20.0 and r["location_name"] == ""


def test_tempest_nws_rows():
    alerts = {"features": [
        {"properties": {"event": "Tornado Warning", "severity": "Extreme", "headline": "h", "urgency": "Immediate",
                        "expires": "2026-11-16T20:00:00-06:00"}},
        {"properties": {"event": "Wind Advisory", "severity": "Moderate"}},
    ]}
    alert_rows, summary = tempest_poller.build_nws_rows("1", "Backyard", None, alerts, now=1234)
    assert [a["event"] for a in alert_rows] == ["Tornado Warning", "Wind Advisory"]
    assert alert_rows[1]["headline"] == "" and alert_rows[1]["urgency"] == "Unknown"
    assert all(a["time"] == 1234 and a["active"] == 1 and a["location_name"] == "" for a in alert_rows)
    assert summary == {"time": 1234, "station_id": "1", "station_name": "Backyard", "location_name": "",
                       "alert_count": 2, "max_severity_rank": 4}
    for a in alert_rows:
        chs.TEMPEST_NWS_ALERTS.row_values(a)
    chs.TEMPEST_NWS_ALERT_SUMMARY.row_values(summary)

    _, empty = tempest_poller.build_nws_rows("1", "", "", {"features": []}, now=1)
    assert empty["alert_count"] == 0 and empty["max_severity_rank"] == 0


def test_tempest_write_batch_and_healthcheck(fake_clickhouse, monkeypatch):
    monkeypatch.setattr(tempest_poller, "CLICKHOUSE_DATABASE", "tempest")
    monkeypatch.setattr(tempest_poller, "CLICKHOUSE_AUTO_CREATE_SCHEMA", True)
    ok, msg = tempest_poller.check_clickhouse(fake_clickhouse)
    assert ok, msg
    assert any("`tempest`.`tempest_observations`" in c for c in fake_clickhouse.commands)
    assert ("tempest", "poller_healthcheck", 1) in fake_clickhouse.inserts

    rows = tempest_poller.build_rows(TEMPEST_RESPONSE, "999")
    alerts, summary = tempest_poller.build_nws_rows("123456", "Backyard", "", {"features": []}, now=5)
    assert tempest_poller.write_batch(fake_clickhouse, rows, alerts, [summary]) == 2
    # Same observation polled again collapses (ReplacingMergeTree identity)
    tempest_poller.write_batch(fake_clickhouse, rows, [], [])
    assert len(fake_clickhouse.data[("tempest", "tempest_observations")]) == 1


def test_tempest_check_clickhouse_reports_insert_failure(fake_clickhouse, monkeypatch):
    monkeypatch.setattr(tempest_poller, "CLICKHOUSE_AUTO_CREATE_SCHEMA", False)

    def boom(*a, **k):
        raise RuntimeError("ACCESS_DENIED")

    monkeypatch.setattr(fake_clickhouse, "insert", boom)
    ok, msg = tempest_poller.check_clickhouse(fake_clickhouse)
    assert not ok and "ACCESS_DENIED" in msg and fake_clickhouse.commands == []


# ---------------------------------------------------------------------------
# OWM poller
# ---------------------------------------------------------------------------

OWM_RESPONSE = {
    "coord": {"lon": -104.98, "lat": 39.74},
    "weather": [{"main": "Clouds", "description": "broken clouds"}],
    "main": {"temp": 50.0, "feels_like": 48.0, "pressure": 1013, "humidity": 40},
    "visibility": 10000,
    "wind": {"speed": 5.0, "deg": 200},
    "rain": {"1h": 2.54},
    "clouds": {"all": 75},
    "dt": 1760000123,
    "sys": {"country": "US"},
    "name": "Denver",
}


def test_owm_build_row_imperial(monkeypatch):
    monkeypatch.setattr(owm_poller, "OWM_UNITS", "imperial")
    r = owm_poller.build_row("Denver,CO,US", OWM_RESPONSE)
    assert r["time"] == 1760000123 and r["location"] == "Denver,CO,US"
    assert (r["city_name"], r["state"], r["country"]) == ("Denver", "CO", "US")
    assert (r["weather_main"], r["weather_description"]) == ("Clouds", "broken clouds")
    assert r["pressure"] == pytest.approx(29.9139, rel=1e-4)
    assert r["visibility"] == pytest.approx(6.2137, rel=1e-4)
    assert r["rain_1h"] == pytest.approx(0.1)
    assert r["latitude"] == 39.74 and "wind_gust" not in r and "snow_1h" not in r
    chs.OWM_OBSERVATIONS.row_values(r)


def test_owm_build_row_metric_and_missing_dt(monkeypatch):
    monkeypatch.setattr(owm_poller, "OWM_UNITS", "metric")
    data = dict(OWM_RESPONSE, dt=None, weather=[])
    r = owm_poller.build_row("Paris,FR", data)
    assert r["pressure"] == 1013.0 and r["visibility"] == 10000.0
    assert r["state"] == "" and r["weather_main"] == "" and r["time"] > 1760000123


def test_owm_nws_rows_and_write(fake_clickhouse, monkeypatch):
    monkeypatch.setattr(owm_poller, "CLICKHOUSE_DATABASE", "weathermap")
    alerts, summary = owm_poller.build_nws_rows(
        "Miami,FL,US", "Miami", "FL", "US",
        {"features": [{"properties": {"event": "Hurricane Warning", "severity": "Extreme"}}]}, now=99,
    )
    assert alerts[0]["state"] == "FL" and summary["max_severity_rank"] == 4
    row = owm_poller.build_row("Denver,CO,US", OWM_RESPONSE)
    assert owm_poller.write_batch(fake_clickhouse, [row], alerts, [summary]) == 3
