#!/usr/bin/env python3
"""
Tempest Weather Station Poller
Polls the WeatherFlow Tempest cloud REST API for one or more stations and
writes observations to InfluxDB 2.x. Each station's human-readable location
name comes from a manual override map (STATION_LOCATION_OVERRIDES) - there
is no automatic geocoding.

Configuration is via environment variables (see tempest-poller.env.example):
  TEMPEST_TOKEN        - WeatherFlow personal access token
  TEMPEST_STATION_IDS  - Comma-separated list of station IDs, e.g. "123456,789012"
  TEMPEST_STATION_ID   - Legacy single-station variable (used if _IDS not set)
  INFLUX_URL           - e.g. http://localhost:8086
  INFLUX_TOKEN         - InfluxDB API token with write access
  INFLUX_ORG           - InfluxDB organization name
  INFLUX_BUCKET        - InfluxDB bucket name
  POLL_INTERVAL        - Seconds between polls (default 60)
  STATION_LOCATION_OVERRIDES - Manual location names, one entry per station.
                         Format: "station_id:Location Name;station_id2:Other Location"
                         A station with no entry here simply has no location_name tag.
  NWS_ALERTS_ENABLED   - "true"/"false" - poll National Weather Service alerts (default true)
  NWS_POLL_INTERVAL    - Seconds between NWS alert checks (default 300 - alerts don't
                         need minute-level freshness, and this is polite to NWS's API)
  NWS_USER_AGENT       - Required by NWS API policy - identify your app/contact
  TEMPEST_UNITS        - "imperial" (default) converts C/m/s/hPa/mm to F/mph/inHg/in
                         at write time; "metric" stores the API's raw values as-is

Release notes:
  v1.9 - Fixed pressure_trend being stored as a tag instead of a field. Since
         it changes with actual conditions (steady/rising/falling), tagging
         it fragmented series identity - every time it changed, the station
         appeared to start a "new" series, causing duplicate tiles/legend
         entries in Grafana until the old tag-combination's data aged out.
         It's now a string field, matching how it should have been from the
         start. Requires a bucket wipe to clear already-fragmented historical
         tag combinations.
  v1.8 - Removed reverse geocoding (OpenStreetMap Nominatim) entirely. Location
         names now come only from STATION_LOCATION_OVERRIDES - no external
         geocoding dependency, no zoom-level guesswork, no cache/refresh
         logic. A station without an override entry simply has no
         location_name tag.
  v1.7 - Fixed a unit mismatch: the Tempest REST API always returns raw metric
         values (Celsius, m/s, hPa, mm) regardless of the station's configured
         display units - the station_units field in the API response reflects
         only the WeatherFlow app's display preference, not the actual units
         of the obs data. All temperature/wind/pressure/precip/lightning-distance
         fields are now converted to imperial (F/mph/inHg/in/mi) at write time
         via TEMPEST_UNITS, matching what the dashboard's unit labels already
         claimed. Set TEMPEST_UNITS=metric to disable conversion and store raw
         values instead.
  v1.6 - Added National Weather Service severe weather alert polling. For each
         station's coordinates, checks api.weather.gov for active alerts
         (Tornado/Severe Thunderstorm/Flash Flood Warnings, etc.) every
         NWS_POLL_INTERVAL seconds (default 5min, independent of the main
         60s weather poll). Writes a per-station alert_count/max_severity_rank
         summary point plus one detail point per active alert.
  v1.5 - Added STATION_LOCATION_OVERRIDES env var: a manual per-station location
         name map that bypasses reverse geocoding entirely for stations where
         the auto-resolved name isn't what you want (e.g. rural coordinates
         resolving to a small unincorporated community instead of the nearest
         town people actually know it by).
  v1.4 - Reverted default GEOCODE_ZOOM from 10 back to 14. Zoom=10 (county-level)
         returns nothing but a county name for rural/exurban coordinates with no
         nearby incorporated city - it doesn't fall back to a "nearby recognizable
         city" the way a lower zoom might suggest. Zoom=14 (suburb/neighborhood)
         correctly resolves to the actual nearest named place (e.g. an
         unincorporated community), which is what most home stations need.
  v1.3 - Reverse-geocoded location_name now returns "City, State (Country)" only,
         with no full-address fallback string. (Temporarily lowered default
         GEOCODE_ZOOM to 10 in this version - corrected in v1.4 above.)
  v1.2 - Added multi-station support (TEMPEST_STATION_IDS, comma-separated).
         Added reverse geocoding of each station's GPS coordinates into a
         location_name tag via OpenStreetMap Nominatim, cached and refreshed on
         GEOCODE_REFRESH_HOURS rather than looked up every poll. Startup Tempest
         API check now tolerates individual station failures, only failing hard
         if every configured station is unreachable. latitude/longitude are now
         written as fields on every point (enables map panels in Grafana).
  v1.1 - Rewrote build_points() to parse the station observation endpoint's
         actual named-JSON response (each obs is a dict with named keys, e.g.
         air_temperature, barometric_pressure, precip, lightning_strike_count)
         instead of the positional obs_st array format, which does not match
         this endpoint. Non-numeric fields (e.g. pressure_trend) become tags
         instead of raising a conversion error.
  v1.0 - Added startup connectivity checks for InfluxDB and the Tempest API,
         with retry/backoff before entering the poll loop. InfluxDB check uses
         a real test write (tempest_poller_healthcheck measurement) rather
         than a bucket-listing lookup, so it works with a write-only scoped
         token that has no read:buckets permission.
  v0.1 - Initial poller: single station, WeatherFlow REST API to InfluxDB 2.x.
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
log = logging.getLogger("tempest-poller")

# ---- Config ----
TEMPEST_TOKEN = os.environ.get("TEMPEST_TOKEN")

_station_ids_raw = os.environ.get("TEMPEST_STATION_IDS") or os.environ.get("TEMPEST_STATION_ID", "")
STATION_IDS = [s.strip() for s in _station_ids_raw.split(",") if s.strip()]

INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "tempest")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))

# Manual per-station location name overrides.
# Format: "station_id:Location Name;station_id2:Other Location", e.g.:
#   STATION_LOCATION_OVERRIDES=123456:Springfield, IL (United States)
_overrides_raw = os.environ.get("STATION_LOCATION_OVERRIDES", "")
STATION_LOCATION_OVERRIDES = {}
for _entry in _overrides_raw.split(";"):
    _entry = _entry.strip()
    if not _entry or ":" not in _entry:
        continue
    _station_id, _name = _entry.split(":", 1)
    STATION_LOCATION_OVERRIDES[_station_id.strip()] = _name.strip()

NWS_ALERTS_ENABLED = os.environ.get("NWS_ALERTS_ENABLED", "true").strip().lower() == "true"
NWS_POLL_INTERVAL = int(os.environ.get("NWS_POLL_INTERVAL", "300"))
NWS_USER_AGENT = os.environ.get(
    "NWS_USER_AGENT", "tempest-poller/1.0 (homelab weather station poller)"
)
NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"

# NWS severity levels, ranked for numeric thresholding/coloring in Grafana
NWS_SEVERITY_RANK = {"Extreme": 4, "Severe": 3, "Moderate": 2, "Minor": 1, "Unknown": 0}

TEMPEST_UNITS = os.environ.get("TEMPEST_UNITS", "imperial").strip().lower()


def _c_to_f(c):
    return c * 9.0 / 5.0 + 32.0


def _c_delta_to_f_delta(c):
    """Convert a temperature *difference* (not an absolute reading) - no +32 offset."""
    return c * 9.0 / 5.0


def _ms_to_mph(ms):
    return ms * 2.236936


def _hpa_to_inhg(hpa):
    return hpa * 0.0295300


def _mm_to_in(mm):
    return mm / 25.4


def _km_to_mi(km):
    return km * 0.621371


# The Tempest REST API always returns raw metric values regardless of the
# station's configured display units - convert to imperial at write time so
# stored values actually match the dashboard's F/mph/inHg/in unit labels.
FIELD_UNIT_CONVERTERS = {
    "air_temperature": _c_to_f,
    "feels_like": _c_to_f,
    "heat_index": _c_to_f,
    "wind_chill": _c_to_f,
    "dew_point": _c_to_f,
    "wet_bulb_temperature": _c_to_f,
    "wet_bulb_globe_temperature": _c_to_f,
    "delta_t": _c_delta_to_f_delta,
    "wind_avg": _ms_to_mph,
    "wind_gust": _ms_to_mph,
    "wind_lull": _ms_to_mph,
    "station_pressure": _hpa_to_inhg,
    "barometric_pressure": _hpa_to_inhg,
    "sea_level_pressure": _hpa_to_inhg,
    "precip": _mm_to_in,
    "precip_accum_last_1hr": _mm_to_in,
    "precip_accum_local_day": _mm_to_in,
    "precip_accum_local_day_final": _mm_to_in,
    "precip_accum_local_yesterday": _mm_to_in,
    "precip_accum_local_yesterday_final": _mm_to_in,
    "lightning_strike_last_distance": _km_to_mi,
}

TEMPEST_API_URL = "https://swd.weatherflow.com/swd/rest/observations/station/{station_id}"

# Non-numeric observation fields that should become string fields rather than
# tags (they change with actual conditions, so tagging them would fragment
# series identity and cause the same station to appear as multiple series)
OBS_STRING_FIELDS = {"pressure_trend"}

# Fields to skip entirely (identifiers/timestamps handled separately)
OBS_SKIP_FIELDS = {"timestamp"}

REQUIRED_ENV = {
    "TEMPEST_TOKEN": TEMPEST_TOKEN,
    "INFLUX_TOKEN": INFLUX_TOKEN,
    "INFLUX_ORG": INFLUX_ORG,
}


def validate_config():
    missing = [k for k, v in REQUIRED_ENV.items() if not v]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)
    if not STATION_IDS:
        log.error("No station IDs configured - set TEMPEST_STATION_IDS (comma-separated) or TEMPEST_STATION_ID")
        sys.exit(1)


def get_location_name(station_id):
    """Return the manually configured location name for a station, if any."""
    return STATION_LOCATION_OVERRIDES.get(station_id)


def fetch_observations(station_id):
    url = TEMPEST_API_URL.format(station_id=station_id)
    params = {"token": TEMPEST_TOKEN}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def build_points(data, fallback_station_id):
    points = []
    station_id = str(data.get("station_id", fallback_station_id))
    station_name = data.get("station_name", "")
    latitude = data.get("latitude")
    longitude = data.get("longitude")

    location_name = get_location_name(station_id)

    for obs in data.get("obs", []):
        if not isinstance(obs, dict):
            log.warning("Skipping unexpected obs entry (not an object): %r", obs)
            continue

        epoch = obs.get("timestamp")
        if epoch is None:
            log.warning("Skipping observation with no timestamp")
            continue

        point = Point("tempest_observations").tag("station_id", station_id)
        if station_name:
            point = point.tag("station_name", station_name)
        if location_name:
            point = point.tag("location_name", location_name)

        if latitude is not None:
            point = point.field("latitude", float(latitude))
        if longitude is not None:
            point = point.field("longitude", float(longitude))

        for key, value in obs.items():
            if key in OBS_SKIP_FIELDS:
                continue
            if value is None:
                continue

            if key in OBS_STRING_FIELDS:
                point = point.field(key, str(value))
                continue

            try:
                float_value = float(value)
                if TEMPEST_UNITS == "imperial" and key in FIELD_UNIT_CONVERTERS:
                    float_value = FIELD_UNIT_CONVERTERS[key](float_value)
                point = point.field(key, float_value)
            except (TypeError, ValueError):
                log.debug("Skipping non-numeric field %s=%r", key, value)

        point = point.time(int(epoch), WritePrecision.S)
        points.append(point)

    return points


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


def build_nws_points(station_id, station_name, location_name, alerts_json):
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
            Point("tempest_nws_alerts")
            .tag("station_id", station_id)
            .tag("event", event)
            .tag("severity", severity)
        )
        if station_name:
            point = point.tag("station_name", station_name)
        if location_name:
            point = point.tag("location_name", location_name)

        point = point.field("headline", props.get("headline", ""))
        point = point.field("urgency", props.get("urgency", "Unknown"))
        point = point.field("expires", props.get("expires", ""))
        point = point.field("active", 1)
        point = point.time(now, WritePrecision.S)
        points.append(point)

    summary_point = Point("tempest_nws_alert_summary").tag("station_id", station_id)
    if station_name:
        summary_point = summary_point.tag("station_name", station_name)
    if location_name:
        summary_point = summary_point.tag("location_name", location_name)
    summary_point = summary_point.field("alert_count", len(features))
    summary_point = summary_point.field("max_severity_rank", max_rank)
    summary_point = summary_point.time(now, WritePrecision.S)
    points.append(summary_point)

    return points


def check_tempest_api():
    """
    Verify the Tempest cloud API is reachable for at least one configured station.
    Returns per-station results so a single misconfigured station doesn't block
    startup for the others, but fails overall if every station is unreachable.
    """
    any_ok = False
    messages = []

    for station_id in STATION_IDS:
        url = TEMPEST_API_URL.format(station_id=station_id)
        params = {"token": TEMPEST_TOKEN}
        try:
            resp = requests.get(url, params=params, timeout=10)
        except requests.exceptions.RequestException as e:
            messages.append(f"station {station_id}: could not reach Tempest API: {e}")
            continue

        if resp.status_code == 401:
            messages.append(f"station {station_id}: token rejected (401 Unauthorized)")
            continue
        if resp.status_code == 404:
            messages.append(f"station {station_id}: not found (404) - check station ID")
            continue
        if not resp.ok:
            messages.append(f"station {station_id}: HTTP {resp.status_code}")
            continue

        try:
            data = resp.json()
        except ValueError:
            messages.append(f"station {station_id}: non-JSON response")
            continue

        if "obs" not in data:
            messages.append(f"station {station_id}: response missing observation data")
            continue

        any_ok = True

    if any_ok:
        return True, "ok" if not messages else f"ok (some stations had issues: {'; '.join(messages)})"
    return False, "; ".join(messages) if messages else "no stations configured"


def check_influxdb(client):
    """
    Verify InfluxDB is reachable and the token can actually write to the bucket.
    Uses a real test write rather than a bucket-listing lookup, since a properly
    least-privilege token is scoped to write-only on one bucket and won't have
    read:buckets permission to list/find bucket metadata.
    """
    try:
        if not client.ping():
            return False, "InfluxDB did not respond to ping"
    except Exception as e:
        return False, f"could not reach InfluxDB: {e}"

    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        check_point = (
            Point("tempest_poller_healthcheck")
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
        tempest_ok, tempest_msg = check_tempest_api()

        if influx_ok and tempest_ok:
            log.info("Startup checks passed: InfluxDB reachable, Tempest API reachable (%s)", tempest_msg)
            return True

        if not influx_ok:
            log.warning("[Attempt %d/%d] InfluxDB check failed: %s", attempt, max_attempts, influx_msg)
        if not tempest_ok:
            log.warning("[Attempt %d/%d] Tempest API check failed: %s", attempt, max_attempts, tempest_msg)

        if attempt < max_attempts:
            delay = base_delay * attempt
            log.info("Retrying startup checks in %ds...", delay)
            time.sleep(delay)

    return False


def main():
    validate_config()
    log.info(
        "Starting Tempest poller: stations=%s influx=%s bucket=%s interval=%ss units=%s",
        ",".join(STATION_IDS), INFLUX_URL, INFLUX_BUCKET, POLL_INTERVAL, TEMPEST_UNITS,
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

        for station_id in STATION_IDS:
            try:
                data = fetch_observations(station_id)
                points = build_points(data, fallback_station_id=station_id)
                all_points.extend(points)

                if do_nws_poll:
                    resolved_id = str(data.get("station_id", station_id))
                    lat = data.get("latitude")
                    lon = data.get("longitude")
                    if lat is not None and lon is not None:
                        try:
                            station_name = data.get("station_name", "")
                            location_name = get_location_name(resolved_id)
                            alerts_json = fetch_nws_alerts(lat, lon)
                            nws_points = build_nws_points(resolved_id, station_name, location_name, alerts_json)
                            all_points.extend(nws_points)
                        except requests.exceptions.RequestException as e:
                            log.warning("NWS alerts fetch failed for station %s: %s", station_id, e)
                        except Exception as e:
                            log.exception("Unexpected error fetching NWS alerts for station %s: %s", station_id, e)
            except requests.exceptions.RequestException as e:
                log.error("Tempest API request failed for station %s: %s", station_id, e)
            except Exception as e:
                log.exception("Unexpected error polling station %s: %s", station_id, e)

        if do_nws_poll:
            last_nws_poll = time.time()

        if all_points:
            try:
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=all_points)
                log.info("Wrote %d point(s) to InfluxDB across %d station(s)", len(all_points), len(STATION_IDS))
            except Exception as e:
                log.exception("Failed to write points to InfluxDB: %s", e)
        else:
            log.warning("No observations collected this poll across any station")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
