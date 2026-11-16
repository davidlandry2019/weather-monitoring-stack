#!/usr/bin/env python3
"""
Shared ClickHouse schema + connection helpers for the weather monitoring stack.

Used by tempest_poller.py, owm_poller.py and migrate_influx_to_clickhouse.py,
so the table definitions live in exactly one place. Copy this file next to
whichever poller you deploy.

Run it directly to create or print the schema:

  python3 clickhouse_schema.py create  --pipeline tempest [--database tempest]
  python3 clickhouse_schema.py create  --pipeline owm     [--database weathermap]
  python3 clickhouse_schema.py print   --pipeline tempest  > tempest.sql

Connection settings come from the same environment variables the pollers use:
  CLICKHOUSE_HOST      - default localhost
  CLICKHOUSE_PORT      - HTTP(S) interface port, default 8123 (8443 is typical with TLS)
  CLICKHOUSE_USER      - default "default"
  CLICKHOUSE_PASSWORD  - default empty
  CLICKHOUSE_SECURE    - "true" to use HTTPS, default false
  CLICKHOUSE_DATABASE  - default "tempest" / "weathermap" depending on pipeline

Design notes:
  * Wide tables: one row per observation, one column per field. Tag-like
    values (station_id, station_name, location_name, ...) are stored on every
    row - there's no separate dimension table to join.
  * Every data table is a ReplacingMergeTree keyed on the same identity
    InfluxDB used to deduplicate points (series + timestamp). Re-writing the
    same observation - the poller re-reading an unchanged latest obs, or the
    migration tool being re-run over a window - collapses to one row.
    Background merges do that collapse eventually; queries that SUM or COUNT
    use FINAL so they're exact immediately.
  * Table names match the old InfluxDB measurement names
    (tempest_observations, owm_nws_alerts, ...) so the two pipelines can share
    a single database if you prefer that to the default two.
  * All timestamps are DateTime('UTC'), second precision - matching what the
    pollers always wrote to InfluxDB.
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple  # ((column_name, clickhouse_type), ...) - the insertable columns
    key: tuple  # ORDER BY / dedup identity (ReplacingMergeTree)
    engine: str = "ReplacingMergeTree(inserted_at)"
    partition_by: str = "toYYYYMM(time)"
    ttl: str = ""
    # The InfluxDB measurement this table replaces (None = not migrated)
    influx_measurement: str = None
    # Columns that held tag values in InfluxDB (strings, '' when absent)
    tag_columns: tuple = ()
    versioned: bool = True  # adds inserted_at DateTime64 version column

    @property
    def column_names(self):
        return [c for c, _ in self.columns]

    def column_type(self, column):
        for c, t in self.columns:
            if c == column:
                return t
        raise KeyError(column)

    def row_values(self, row):
        """
        Turn a {column: value} dict into a list in column order, filling
        anything missing with the column's natural empty value ('' for
        strings, NULL for Nullable, {} for maps, 0 for integers). `time` must
        be an int epoch-seconds value and is required.
        """
        if row.get("time") is None:
            raise ValueError(f"{self.name}: row has no time")
        return [row[c] if row.get(c) is not None else empty_value(t) for c, t in self.columns]

    def ddl(self, database):
        cols = [f"    {quote_ident(c)} {t}" for c, t in self.columns]
        if self.versioned:
            cols.append("    `inserted_at` DateTime64(3, 'UTC') DEFAULT now64(3)")
        lines = [
            f"CREATE TABLE IF NOT EXISTS {quote_ident(database)}.{quote_ident(self.name)}",
            "(",
            ",\n".join(cols),
            ")",
            f"ENGINE = {self.engine}",
        ]
        if self.partition_by:
            lines.append(f"PARTITION BY {self.partition_by}")
        lines.append(f"ORDER BY ({', '.join(quote_ident(k) for k in self.key)})")
        if self.ttl:
            lines.append(f"TTL {self.ttl}")
        return "\n".join(lines)


TS = "DateTime('UTC')"
TAG = "LowCardinality(String)"
F64 = "Nullable(Float64)"
EXTRA = "Map(LowCardinality(String), Float64)"

# Numeric fields returned by the Tempest station observation endpoint. Anything
# the API adds later that isn't listed here still gets stored - in the `extra`
# map column - so no data is silently dropped.
TEMPEST_FLOAT_FIELDS = (
    "air_temperature",
    "barometric_pressure",
    "station_pressure",
    "sea_level_pressure",
    "relative_humidity",
    "precip",
    "precip_accum_last_1hr",
    "precip_accum_local_day",
    "precip_accum_local_day_final",
    "precip_accum_local_yesterday",
    "precip_accum_local_yesterday_final",
    "precip_minutes_local_day",
    "precip_minutes_local_yesterday",
    "precip_minutes_local_yesterday_final",
    "precip_analysis_type_yesterday",
    "solar_radiation",
    "uv",
    "brightness",
    "lightning_strike_count",
    "lightning_strike_count_last_1hr",
    "lightning_strike_count_last_3hr",
    "lightning_strike_last_distance",
    "lightning_strike_last_epoch",
    "wind_lull",
    "wind_avg",
    "wind_gust",
    "wind_direction",
    "feels_like",
    "heat_index",
    "wind_chill",
    "dew_point",
    "wet_bulb_temperature",
    "wet_bulb_globe_temperature",
    "delta_t",
    "air_density",
)

TEMPEST_OBSERVATIONS = Table(
    name="tempest_observations",
    influx_measurement="tempest_observations",
    tag_columns=("station_id", "station_name", "location_name"),
    key=("station_id", "time"),
    columns=(
        ("time", TS),
        ("station_id", TAG),
        ("station_name", TAG),
        ("location_name", TAG),
        ("latitude", F64),
        ("longitude", F64),
        *((f, F64) for f in TEMPEST_FLOAT_FIELDS),
        ("pressure_trend", TAG),
        ("extra", EXTRA),
    ),
)

TEMPEST_NWS_ALERTS = Table(
    name="tempest_nws_alerts",
    influx_measurement="tempest_nws_alerts",
    tag_columns=("station_id", "station_name", "location_name", "event", "severity"),
    key=("station_id", "event", "severity", "time"),
    columns=(
        ("time", TS),  # when the poller saw the alert as active
        ("station_id", TAG),
        ("station_name", TAG),
        ("location_name", TAG),
        ("event", TAG),
        ("severity", TAG),
        ("urgency", TAG),
        ("headline", "String"),
        ("expires", "String"),  # ISO-8601 with offset, exactly as NWS returns it
        ("active", "UInt8"),
    ),
)

TEMPEST_NWS_ALERT_SUMMARY = Table(
    name="tempest_nws_alert_summary",
    influx_measurement="tempest_nws_alert_summary",
    tag_columns=("station_id", "station_name", "location_name"),
    key=("station_id", "time"),
    columns=(
        ("time", TS),
        ("station_id", TAG),
        ("station_name", TAG),
        ("location_name", TAG),
        ("alert_count", "UInt32"),
        ("max_severity_rank", "UInt8"),
    ),
)

OWM_FLOAT_FIELDS = (
    "temp",
    "feels_like",
    "temp_min",
    "temp_max",
    "humidity",
    "wind_speed",
    "wind_deg",
    "wind_gust",
    "clouds_all",
    "latitude",
    "longitude",
    "pressure",
    "visibility",
    "rain_1h",
    "snow_1h",
)

OWM_OBSERVATIONS = Table(
    name="owm_observations",
    influx_measurement="owm_observations",
    tag_columns=("location", "city_name", "state", "country"),
    key=("location", "time"),
    columns=(
        ("time", TS),
        ("location", TAG),
        ("city_name", TAG),
        ("state", TAG),
        ("country", TAG),
        ("weather_main", TAG),
        ("weather_description", TAG),
        *((f, F64) for f in OWM_FLOAT_FIELDS),
        ("extra", EXTRA),
    ),
)

OWM_NWS_ALERTS = Table(
    name="owm_nws_alerts",
    influx_measurement="owm_nws_alerts",
    tag_columns=("location", "city_name", "state", "country", "event", "severity"),
    key=("location", "event", "severity", "time"),
    columns=(
        ("time", TS),
        ("location", TAG),
        ("city_name", TAG),
        ("state", TAG),
        ("country", TAG),
        ("event", TAG),
        ("severity", TAG),
        ("urgency", TAG),
        ("headline", "String"),
        ("expires", "String"),
        ("active", "UInt8"),
    ),
)

OWM_NWS_ALERT_SUMMARY = Table(
    name="owm_nws_alert_summary",
    influx_measurement="owm_nws_alert_summary",
    tag_columns=("location", "city_name", "state", "country"),
    key=("location", "time"),
    columns=(
        ("time", TS),
        ("location", TAG),
        ("city_name", TAG),
        ("state", TAG),
        ("country", TAG),
        ("alert_count", "UInt32"),
        ("max_severity_rank", "UInt8"),
    ),
)

# Startup write-permission check target (replaces the *_poller_healthcheck
# measurements). Self-expiring, never migrated.
POLLER_HEALTHCHECK = Table(
    name="poller_healthcheck",
    key=("poller", "time"),
    engine="MergeTree",
    partition_by="",
    ttl="time + INTERVAL 30 DAY",
    versioned=False,
    columns=(
        ("time", TS),
        ("poller", TAG),
        ("detail", "String"),
    ),
)

PIPELINES = {
    "tempest": {
        "default_database": "tempest",
        "default_bucket": "tempest",
        "tables": (TEMPEST_OBSERVATIONS, TEMPEST_NWS_ALERTS, TEMPEST_NWS_ALERT_SUMMARY, POLLER_HEALTHCHECK),
    },
    "owm": {
        "default_database": "weathermap",
        "default_bucket": "weathermap",
        "tables": (OWM_OBSERVATIONS, OWM_NWS_ALERTS, OWM_NWS_ALERT_SUMMARY, POLLER_HEALTHCHECK),
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_ident(name):
    """Database/table names are interpolated into SQL - only allow plain identifiers."""
    if not name or not _IDENT_RE.match(name):
        raise ValueError(f"invalid ClickHouse identifier {name!r} (letters, digits, underscore only)")
    return name


def quote_ident(name):
    return f"`{validate_ident(name)}`"


def empty_value(ch_type):
    if ch_type.startswith("Nullable("):
        return None
    if ch_type.startswith("Map("):
        return {}
    if "String" in ch_type:
        return ""
    if ch_type.startswith(("UInt", "Int", "Float")):
        return 0
    raise ValueError(f"no empty value for type {ch_type}")


def insert_rows(client, database, table, rows):
    """Insert a list of {column: value} dicts into one table. Returns the row count."""
    if not rows:
        return 0
    client.insert(
        table.name,
        [table.row_values(r) for r in rows],
        column_names=table.column_names,
        database=database,
    )
    return len(rows)


def schema_sql(pipeline, database):
    validate_ident(database)
    stmts = [f"CREATE DATABASE IF NOT EXISTS {quote_ident(database)}"]
    stmts += [t.ddl(database) for t in PIPELINES[pipeline]["tables"]]
    return stmts


def ensure_schema(client, pipeline, database):
    """CREATE ... IF NOT EXISTS for the database and every table of a pipeline."""
    for stmt in schema_sql(pipeline, database):
        client.command(stmt)


def env_bool(name, default):
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def clickhouse_settings_from_env():
    secure = env_bool("CLICKHOUSE_SECURE", False)
    return {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.environ.get("CLICKHOUSE_PORT", "8443" if secure else "8123")),
        "username": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "secure": secure,
    }


def get_client(settings=None):
    import clickhouse_connect  # imported lazily so `print` works without the dependency

    settings = settings or clickhouse_settings_from_env()
    return clickhouse_connect.get_client(**settings)


def describe(settings):
    scheme = "https" if settings["secure"] else "http"
    return f"{scheme}://{settings['username']}@{settings['host']}:{settings['port']}"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Create or print the weather stack's ClickHouse schema")
    parser.add_argument("action", choices=("create", "print"))
    parser.add_argument("--pipeline", choices=sorted(PIPELINES), required=True)
    parser.add_argument(
        "--database",
        help="target database (default: $CLICKHOUSE_DATABASE, else tempest / weathermap)",
    )
    args = parser.parse_args(argv)

    database = args.database or os.environ.get("CLICKHOUSE_DATABASE") or PIPELINES[args.pipeline]["default_database"]
    stmts = schema_sql(args.pipeline, database)

    if args.action == "print":
        print(";\n\n".join(stmts) + ";")
        return 0

    settings = clickhouse_settings_from_env()
    client = get_client(settings)
    for stmt in stmts:
        client.command(stmt)
    print(f"Schema for pipeline '{args.pipeline}' ensured in database '{database}' on {describe(settings)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
