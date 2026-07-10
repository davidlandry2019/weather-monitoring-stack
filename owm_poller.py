#!/usr/bin/env python3
"""
OpenWeatherMap Poller
Polls the OpenWeatherMap free-tier Current Weather Data API for one or more
named locations and writes observations to InfluxDB 2.x.

Configuration is via environment variables (see owm-poller.env.example):
  OWM_API_KEY      - Your OpenWeatherMap API key
  OWM_LOCATIONS    - Semicolon-separated list of locations in OWM's
                     "City,State,Country" query format, e.g.:
                     "Denver,CO,US;Miami,FL,US;Seattle,WA,US"
  OWM_UNITS        - "imperial" (F/mph, default) or "metric" (C/m/s) -
                     note: OWM always returns pressure in hPa and visibility
                     in meters regardless of this setting; those are
                     converted to inHg/miles here when OWM_UNITS=imperial.
  INFLUX_URL       - e.g. http://localhost:8086
  INFLUX_TOKEN     - InfluxDB API token with write access
  INFLUX_ORG       - InfluxDB organization name
  INFLUX_BUCKET    - InfluxDB bucket name (default: weathermap - kept separate
                     from the Tempest bucket since this is unrelated,
                     arbitrary-location data rather than your own station)
  POLL_INTERVAL    - Seconds between polls (default 300 - OWM's underlying
                     station/model data typically doesn't refresh faster
                     than every 10min-1hr regardless of poll frequency)
  NWS_ALERTS_ENABLED - "true"/"false" - poll National Weather Service alerts
                     for each location's coordinates (default true)
  NWS_POLL_INTERVAL- Seconds between NWS alert checks (default 300)
  NWS_USER_AGENT   - Required by NWS API policy - identify your app/contact

Release notes:
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
from datetime import datetime

import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

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

INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "weathermap")
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
    "INFLUX_TOKEN": INFLUX_TOKEN,
    "INFLUX_ORG": INFLUX_ORG,
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


def build_points(location_query, data):
    main = data.get("main", {})
    wind = data.get("wind", {})
    clouds = data.get("clouds", {})
    rain = data.get("rain", {})
    snow = data.get("snow", {})
    sys_info = data.get("sys", {})
    weather_list = data.get("weather", [])
    weather = weather_list[0] if weather_list else {}

    city_name = data.get("name", location_query)
    country = sys_info.get("country", "")
    coord = data.get("coord", {})
    state = parse_state(location_query)

    point = Point("owm_observations").tag("location", location_query)
    if city_name:
        point = point.tag("city_name", city_name)
    if state:
        point = point.tag("state", state)
    if country:
        point = point.tag("country", country)
    if weather.get("main"):
        point = point.field("weather_main", weather["main"])
    if weather.get("description"):
        point = point.field("weather_description", weather["description"])

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
            point = point.field(field, float(value))
        except (TypeError, ValueError):
            log.debug("Skipping non-numeric field %s=%r for %s", field, value, location_query)

    # Fields OWM always returns in fixed units regardless of the `units` param
    pressure_hpa = main.get("pressure")
    if pressure_hpa is not None:
        pressure = _hpa_to_inhg(pressure_hpa) if OWM_UNITS == "imperial" else pressure_hpa
        point = point.field("pressure", float(pressure))

    visibility_m = data.get("visibility")
    if visibility_m is not None:
        visibility = _m_to_mi(visibility_m) if OWM_UNITS == "imperial" else visibility_m
        point = point.field("visibility", float(visibility))

    rain_1h = rain.get("1h")
    if rain_1h is not None:
        point = point.field("rain_1h", float(_mm_to_in(rain_1h) if OWM_UNITS == "imperial" else rain_1h))

    snow_1h = snow.get("1h")
    if snow_1h is not None:
        point = point.field("snow_1h", float(_mm_to_in(snow_1h) if OWM_UNITS == "imperial" else snow_1h))

    dt = data.get("dt")
    if dt is not None:
        point = point.time(int(dt), WritePrecision.S)

    return point


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


def build_nws_points(location_query, city_name, state, country, alerts_json):
    """
    Build InfluxDB points from an NWS alerts response: one summary point
    (alert_count, max_severity_rank) for easy dashboard threshold coloring,
    plus one detail point per currently active alert.
    """
    points = []
    features = alerts_json.get("features", [])
    now = datetime.utcnow()
    max_rank = 0

    for feature in features:
        props = feature.get("properties", {})
        event = props.get("event", "Unknown")
        severity = props.get("severity", "Unknown")
        max_rank = max(max_rank, NWS_SEVERITY_RANK.get(severity, 0))

        point = (
            Point("owm_nws_alerts")
            .tag("location", location_query)
            .tag("event", event)
            .tag("severity", severity)
        )
        if city_name:
            point = point.tag("city_name", city_name)
        if state:
            point = point.tag("state", state)
        if country:
            point = point.tag("country", country)

        point = point.field("headline", props.get("headline", ""))
        point = point.field("urgency", props.get("urgency", "Unknown"))
        point = point.field("expires", props.get("expires", ""))
        point = point.field("active", 1)
        point = point.time(now, WritePrecision.S)
        points.append(point)

    summary_point = Point("owm_nws_alert_summary").tag("location", location_query)
    if city_name:
        summary_point = summary_point.tag("city_name", city_name)
    if state:
        summary_point = summary_point.tag("state", state)
    if country:
        summary_point = summary_point.tag("country", country)
    summary_point = summary_point.field("alert_count", len(features))
    summary_point = summary_point.field("max_severity_rank", max_rank)
    summary_point = summary_point.time(now, WritePrecision.S)
    points.append(summary_point)

    return points


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


def check_influxdb(client):
    """Verify InfluxDB is reachable and the token can actually write to the bucket."""
    try:
        if not client.ping():
            return False, "InfluxDB did not respond to ping"
    except Exception as e:
        return False, f"could not reach InfluxDB: {e}"

    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        check_point = (
            Point("owm_poller_healthcheck")
            .tag("check", "startup")
            .field("value", 1)
        )
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=check_point)
    except Exception as e:
        return False, (
            f"test write to bucket '{INFLUX_BUCKET}' in org '{INFLUX_ORG}' failed: {e} "
            "(check token write permission, org name, and bucket name)"
        )

    return True, "ok"


def run_startup_checks(client, max_attempts=5, base_delay=5):
    """Run connectivity checks before entering the poll loop, retrying with backoff."""
    for attempt in range(1, max_attempts + 1):
        influx_ok, influx_msg = check_influxdb(client)
        owm_ok, owm_msg = check_owm_api()

        if influx_ok and owm_ok:
            log.info("Startup checks passed: InfluxDB reachable, OpenWeatherMap API reachable (%s)", owm_msg)
            return True

        if not influx_ok:
            log.warning("[Attempt %d/%d] InfluxDB check failed: %s", attempt, max_attempts, influx_msg)
        if not owm_ok:
            log.warning("[Attempt %d/%d] OpenWeatherMap check failed: %s", attempt, max_attempts, owm_msg)

        if attempt < max_attempts:
            delay = base_delay * attempt
            log.info("Retrying startup checks in %ds...", delay)
            time.sleep(delay)

    return False


def main():
    validate_config()
    log.info(
        "Starting OpenWeatherMap poller: locations=%s influx=%s bucket=%s interval=%ss units=%s nws_alerts=%s",
        "; ".join(OWM_LOCATIONS), INFLUX_URL, INFLUX_BUCKET, POLL_INTERVAL, OWM_UNITS, NWS_ALERTS_ENABLED,
    )

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

    if not run_startup_checks(client):
        log.error("Startup checks failed after multiple attempts - exiting so systemd can restart/back off")
        client.close()
        sys.exit(1)

    write_api = client.write_api(write_options=SYNCHRONOUS)
    last_nws_poll = 0.0

    while True:
        all_points = []
        do_nws_poll = NWS_ALERTS_ENABLED and (time.time() - last_nws_poll >= NWS_POLL_INTERVAL)

        for location_query in OWM_LOCATIONS:
            try:
                data = fetch_weather(location_query)
                point = build_points(location_query, data)
                all_points.append(point)

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
                            nws_points = build_nws_points(location_query, city_name, state, country, alerts_json)
                            all_points.extend(nws_points)
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

        if all_points:
            try:
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=all_points)
                log.info("Wrote %d point(s) to InfluxDB across %d location(s)", len(all_points), len(OWM_LOCATIONS))
            except Exception as e:
                log.exception("Failed to write points to InfluxDB: %s", e)
        else:
            log.warning("No observations collected this poll across any location")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
