#!/usr/bin/env python3
"""
OpenWeatherMap Poller
Polls the OpenWeatherMap free-tier Current Weather Data API for one or more
named locations and writes observations to ClickHouse.

Configuration is via environment variables (see owm-poller.env.example):
  OWM_API_KEY      - Your OpenWeatherMap API key
  OWM_LOCATIONS    - Semicolon-separated list of locations in OWM's
                     "City,State,Country" query format, e.g.:
                     "Denver,CO,US;Miami,FL,US;Seattle,WA,US"
  OWM_UNITS        - "imperial" (F/mph, default) or "metric" (C/m/s) -
                     note: OWM always returns pressure in hPa and visibility
                     in meters regardless of this setting; those are
                     converted to inHg/miles here when OWM_UNITS=imperial.
  CLICKHOUSE_HOST  - default localhost
  CLICKHOUSE_PORT  - HTTP interface port, default 8123 (8443 with CLICKHOUSE_SECURE)
  CLICKHOUSE_USER  - ClickHouse user with INSERT on the database (default "default")
  CLICKHOUSE_PASSWORD - that user's password
  CLICKHOUSE_SECURE - "true" to connect over HTTPS (default false)
  CLICKHOUSE_DATABASE - database name (default: weathermap - kept separate
                     from the Tempest database since this is unrelated,
                     arbitrary-location data rather than your own station;
                     table names don't collide, so sharing one is also fine)
  CLICKHOUSE_AUTO_CREATE_SCHEMA - "true" (default) runs CREATE ... IF NOT
                     EXISTS for the database/tables at startup; set "false"
                     for an INSERT-only user and create the schema yourself
                     with clickhouse_schema.py
  POLL_INTERVAL    - Seconds between polls (default 300 - OWM's underlying
                     station/model data typically doesn't refresh faster
                     than every 10min-1hr regardless of poll frequency)
  NWS_ALERTS_ENABLED - "true"/"false" - poll National Weather Service alerts
                     for each location's coordinates (default true)
  NWS_POLL_INTERVAL- Seconds between NWS alert checks (default 300)
  NWS_USER_AGENT   - Required by NWS API policy - identify your app/contact

Release notes:
  v2.0 - Storage moved from InfluxDB 2.x to ClickHouse (InfluxData has put
         Flux, which the dashboards were written in, into maintenance mode,
         and InfluxDB 3 doesn't run it at all). Rows go to the
         owm_observations / owm_nws_alerts / owm_nws_alert_summary tables
         defined in clickhouse_schema.py - one wide row per observation,
         same field names and units as before. The startup write check now
         inserts into poller_healthcheck. INFLUX_* variables are replaced by
         CLICKHOUSE_*. Historical data can be copied over with
         migrate_influx_to_clickhouse.py.
  v1.4 - Fixed weather_main and weather_description being stored as tags
         instead of fields. Since they change with actual weather conditions,
         tagging them fragmented series identity - every time conditions
         changed, a city appeared to start a "new" series, causing duplicate
         tiles/legend entries in Grafana until the old tag-combination's data
         aged out. They're now string fields. Requires a bucket wipe to clear
         already-fragmented historical tag combinations.
  v1.3 - NWS alert points (owm_nws_alerts, owm_nws_alert_summary) now also
         tag state and country, matching owm_observations - the dashboard's
         alert tables were referencing these tags before the poller actually
         wrote them.
  v1.2 - Added a "state" tag, parsed from the configured "City,State,Country"
         OWM_LOCATIONS query string (OWM's own response doesn't include state),
         so dashboard legends can show city/state without needing city_name
         alone to disambiguate.
  v1.1 - Added National Weather Service severe weather alert polling per
         location (Tornado/Severe Thunderstorm/Flash Flood Warnings, etc.),
         same mechanism as the Tempest poller - NWS covers any US
         coordinates, not just personal weather stations.
  v1.0 - Initial poller: multi-location OpenWeatherMap Current Weather Data
         API (free tier) to InfluxDB 2.x, mirroring the Tempest poller's
         structure (startup connectivity checks, per-location resilience,
         imperial unit conversion for the fields OWM doesn't auto-convert).
"""

import os
import sys
import time
import logging

import requests

import clickhouse_schema as chs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("owm-poller")

# ---- Config ----
OWM_API_KEY = os.environ.get("OWM_API_KEY")

_locations_raw = os.environ.get("OWM_LOCATIONS", "")
OWM_LOCATIONS = [loc.strip() for loc in _locations_raw.split(";") if loc.strip()]

OWM_UNITS = os.environ.get("OWM_UNITS", "imperial").strip().lower()

CLICKHOUSE = chs.clickhouse_settings_from_env()
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", chs.PIPELINES["owm"]["default_database"])
CLICKHOUSE_AUTO_CREATE_SCHEMA = chs.env_bool("CLICKHOUSE_AUTO_CREATE_SCHEMA", True)
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "300"))

OWM_API_URL = "https://api.openweathermap.org/data/2.5/weather"

NWS_ALERTS_ENABLED = os.environ.get("NWS_ALERTS_ENABLED", "true").strip().lower() == "true"
NWS_POLL_INTERVAL = int(os.environ.get("NWS_POLL_INTERVAL", "300"))
NWS_USER_AGENT = os.environ.get(
    "NWS_USER_AGENT", "owm-poller/1.0 (homelab weather dashboard poller)"
)
NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"

# NWS severity levels, ranked for numeric thresholding/coloring in Grafana
NWS_SEVERITY_RANK = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1, "Unknown": 0}

REQUIRED_ENV = {
    "OWM_API_KEY": OWM_API_KEY,
}


def _hpa_to_inhg(hpa):
    return hpa * 0.0295300


def _m_to_mi(meters):
    return meters / 1609.344


def _mm_to_in(mm):
    return mm / 25.4


def validate_config():
    missing = [k for k, v in REQUIRED_ENV.items() if not v]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)
    if not OWM_LOCATIONS:
        log.error("No locations configured - set OWM_LOCATIONS (semicolon-separated)")
        sys.exit(1)
    try:
        chs.validate_ident(CLICKHOUSE_DATABASE)
    except ValueError as e:
        log.error("CLICKHOUSE_DATABASE: %s", e)
        sys.exit(1)


def fetch_weather(location_query):
    params = {"q": location_query, "appid": OWM_API_KEY, "units": OWM_UNITS}
    resp = requests.get(OWM_API_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def parse_state(location_query):
    """OWM's response doesn't include state - parse it out of the configured
    'City,State,Country' query string instead."""
    query_parts = [p.strip() for p in location_query.split(",")]
    return query_parts[1] if len(query_parts) >= 3 else ""


def build_row(location_query, data):
    """Turn one Current Weather Data response into an owm_observations row."""
    main = data.get("main", {})
    wind = data.get("wind", {})
    clouds = data.get("clouds", {})
    rain = data.get("rain", {})
    snow = data.get("snow", {})
    sys_info = data.get("sys", {})
    weather_list = data.get("weather", [])
    weather = weather_list[0] if weather_list else {}
    coord = data.get("coord", {})

    dt = data.get("dt")
    row = {
        # OWM's own observation timestamp; fall back to now if it's missing
        "time": int(dt) if dt is not None else int(time.time()),
        "location": location_query,
        "city_name": data.get("name") or location_query,
        "state": parse_state(location_query),
        "country": sys_info.get("country", ""),
        "weather_main": weather.get("main") or "",
        "weather_description": weather.get("description") or "",
    }

    numeric_fields = {
        "temp": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "humidity": main.get("humidity"),
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "wind_gust": wind.get("gust"),
        "clouds_all": clouds.get("all"),
        "latitude": coord.get("lat"),
        "longitude": coord.get("lon"),
    }

    for field, value in numeric_fields.items():
        if value is None:
            continue
        try:
            row[field] = float(value)
        except (TypeError, ValueError):
            log.debug("Skipping non-numeric field %s=%r for %s", field, value, location_query)

    # Fields OWM always returns in fixed units regardless of the `units` param
    pressure_hpa = main.get("pressure")
    if pressure_hpa is not None:
        pressure = _hpa_to_inhg(pressure_hpa) if OWM_UNITS == "imperial" else pressure_hpa
        row["pressure"] = float(pressure)

    visibility_m = data.get("visibility")
    if visibility_m is not None:
        visibility = _m_to_mi(visibility_m) if OWM_UNITS == "imperial" else visibility_m
        row["visibility"] = float(visibility)

    rain_1h = rain.get("1h")
    if rain_1h is not None:
        row["rain_1h"] = float(_mm_to_in(rain_1h) if OWM_UNITS == "imperial" else rain_1h)

    snow_1h = snow.get("1h")
    if snow_1h is not None:
        row["snow_1h"] = float(_mm_to_in(snow_1h) if OWM_UNITS == "imperial" else snow_1h)

    return row


def fetch_nws_alerts(lat, lon):
    """Fetch active NWS alerts (Tornado/Severe Thunderstorm/Flash Flood, etc.) for a point."""
    resp = requests.get(
        NWS_ALERTS_URL,
        params={"point": f"{lat},{lon}"},
        headers={"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def build_nws_rows(location_query, city_name, state, country, alerts_json, now=None):
    """
    Build rows from an NWS alerts response: one detail row per currently
    active alert (owm_nws_alerts) plus one summary row (alert_count,
    max_severity_rank) for easy dashboard threshold coloring
    (owm_nws_alert_summary). Both are stamped with the poll time.
    Returns (alert_rows, summary_row).
    """
    features = alerts_json.get("features", [])
    now = int(time.time()) if now is None else now
    common = {
        "time": now,
        "location": location_query,
        "city_name": city_name or "",
        "state": state or "",
        "country": country or "",
    }
    max_rank = 0
    alert_rows = []

    for feature in features:
        props = feature.get("properties", {})
        severity = props.get("severity") or "Unknown"
        max_rank = max(max_rank, NWS_SEVERITY_RANK.get(severity, 0))
        alert_rows.append({
            **common,
            "event": props.get("event") or "Unknown",
            "severity": severity,
            "headline": props.get("headline") or "",
            "urgency": props.get("urgency") or "Unknown",
            "expires": props.get("expires") or "",
            "active": 1,
        })

    summary_row = {**common, "alert_count": len(features), "max_severity_rank": max_rank}
    return alert_rows, summary_row


def check_owm_api():
    """
    Verify the OpenWeatherMap API is reachable for at least one configured
    location. Tolerant of individual location failures (typo'd city names,
    etc.) - only fails hard if every location is unreachable.
    """
    any_ok = False
    messages = []

    for location_query in OWM_LOCATIONS:
        try:
            resp = requests.get(
                OWM_API_URL,
                params={"q": location_query, "appid": OWM_API_KEY, "units": OWM_UNITS},
                timeout=10,
            )
        except requests.exceptions.RequestException as e:
            messages.append(f"'{location_query}': could not reach OpenWeatherMap API: {e}")
            continue

        if resp.status_code == 401:
            messages.append(f"'{location_query}': API key rejected (401 Unauthorized)")
            continue
        if resp.status_code == 404:
            messages.append(f"'{location_query}': location not found (404) - check spelling/format")
            continue
        if not resp.ok:
            messages.append(f"'{location_query}': HTTP {resp.status_code}")
            continue

        try:
            data = resp.json()
        except ValueError:
            messages.append(f"'{location_query}': non-JSON response")
            continue

        if "main" not in data:
            messages.append(f"'{location_query}': response missing weather data")
            continue

        any_ok = True

    if any_ok:
        return True, "ok" if not messages else f"ok (some locations had issues: {'; '.join(messages)})"
    return False, "; ".join(messages) if messages else "no locations configured"


def check_clickhouse(client):
    """
    Verify ClickHouse is reachable and the user can actually insert into the
    database (real test insert into poller_healthcheck, so it works for an
    INSERT-only user).
    """
    try:
        if not client.ping():
            return False, "ClickHouse did not respond to ping"
    except Exception as e:
        return False, f"could not reach ClickHouse: {e}"

    if CLICKHOUSE_AUTO_CREATE_SCHEMA:
        try:
            chs.ensure_schema(client, "owm", CLICKHOUSE_DATABASE)
        except Exception as e:
            log.warning(
                "Could not create/verify schema in '%s' (%s) - continuing in case it already exists. "
                "Set CLICKHOUSE_AUTO_CREATE_SCHEMA=false if this user intentionally lacks CREATE.",
                CLICKHOUSE_DATABASE, e,
            )

    try:
        chs.insert_rows(client, CLICKHOUSE_DATABASE, chs.POLLER_HEALTHCHECK, [
            {"time": int(time.time()), "poller": "owm", "detail": "startup"},
        ])
    except Exception as e:
        return False, (
            f"test insert into '{CLICKHOUSE_DATABASE}.{chs.POLLER_HEALTHCHECK.name}' failed: {e} "
            "(check the user's INSERT grant, the database name, and that the schema exists)"
        )

    return True, "ok"


def run_startup_checks(client, max_attempts=5, base_delay=5):
    """Run connectivity checks before entering the poll loop, retrying with backoff."""
    for attempt in range(1, max_attempts + 1):
        ch_ok, ch_msg = check_clickhouse(client)
        owm_ok, owm_msg = check_owm_api()

        if ch_ok and owm_ok:
            log.info("Startup checks passed: ClickHouse writable, OpenWeatherMap API reachable (%s)", owm_msg)
            return True

        if not ch_ok:
            log.warning("[Attempt %d/%d] ClickHouse check failed: %s", attempt, max_attempts, ch_msg)
        if not owm_ok:
            log.warning("[Attempt %d/%d] OpenWeatherMap check failed: %s", attempt, max_attempts, owm_msg)

        if attempt < max_attempts:
            delay = base_delay * attempt
            log.info("Retrying startup checks in %ds...", delay)
            time.sleep(delay)

    return False


def write_batch(client, obs_rows, alert_rows, summary_rows):
    written = 0
    written += chs.insert_rows(client, CLICKHOUSE_DATABASE, chs.OWM_OBSERVATIONS, obs_rows)
    written += chs.insert_rows(client, CLICKHOUSE_DATABASE, chs.OWM_NWS_ALERTS, alert_rows)
    written += chs.insert_rows(client, CLICKHOUSE_DATABASE, chs.OWM_NWS_ALERT_SUMMARY, summary_rows)
    return written


def main():
    validate_config()
    log.info(
        "Starting OpenWeatherMap poller: locations=%s clickhouse=%s database=%s interval=%ss units=%s nws_alerts=%s",
        "; ".join(OWM_LOCATIONS), chs.describe(CLICKHOUSE), CLICKHOUSE_DATABASE, POLL_INTERVAL, OWM_UNITS,
        NWS_ALERTS_ENABLED,
    )

    try:
        client = chs.get_client(CLICKHOUSE)
    except Exception as e:
        # get_client() connects immediately; exit so systemd can restart/back off
        log.error("Could not connect to ClickHouse at %s: %s", chs.describe(CLICKHOUSE), e)
        sys.exit(1)

    if not run_startup_checks(client):
        log.error("Startup checks failed after multiple attempts - exiting so systemd can restart/back off")
        client.close()
        sys.exit(1)

    last_nws_poll = 0.0

    while True:
        obs_rows, alert_rows, summary_rows = [], [], []
        do_nws_poll = NWS_ALERTS_ENABLED and (time.time() - last_nws_poll >= NWS_POLL_INTERVAL)

        for location_query in OWM_LOCATIONS:
            try:
                data = fetch_weather(location_query)
                obs_rows.append(build_row(location_query, data))

                if do_nws_poll:
                    coord = data.get("coord", {})
                    lat = coord.get("lat")
                    lon = coord.get("lon")
                    if lat is not None and lon is not None:
                        try:
                            city_name = data.get("name", location_query)
                            state = parse_state(location_query)
                            country = data.get("sys", {}).get("country", "")
                            alerts_json = fetch_nws_alerts(lat, lon)
                            alerts, summary = build_nws_rows(location_query, city_name, state, country, alerts_json)
                            alert_rows.extend(alerts)
                            summary_rows.append(summary)
                        except requests.exceptions.RequestException as e:
                            log.warning("NWS alerts fetch failed for '%s': %s", location_query, e)
                        except Exception as e:
                            log.exception("Unexpected error fetching NWS alerts for '%s': %s", location_query, e)
            except requests.exceptions.RequestException as e:
                log.error("OpenWeatherMap request failed for '%s': %s", location_query, e)
            except Exception as e:
                log.exception("Unexpected error polling '%s': %s", location_query, e)

        if do_nws_poll:
            last_nws_poll = time.time()

        if obs_rows or alert_rows or summary_rows:
            try:
                written = write_batch(client, obs_rows, alert_rows, summary_rows)
                log.info("Wrote %d row(s) to ClickHouse across %d location(s)", written, len(OWM_LOCATIONS))
            except Exception as e:
                log.exception("Failed to write rows to ClickHouse: %s", e)
        else:
            log.warning("No observations collected this poll across any location")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
