#!/usr/bin/env python3
"""
InfluxDB 2.x -> ClickHouse migration tool for the weather monitoring stack.

Copies the history the pollers wrote to InfluxDB (buckets `tempest` and
`weathermap` by default) into the ClickHouse tables defined in
clickhouse_schema.py, then verifies it row by row.

Subcommands
  inspect  Read-only. Shows each measurement's time span, field/tag keys and
           where every key will land in ClickHouse (column, `extra` map, or
           dropped). Run this first.
  migrate  Copies data in time windows (--chunk-hours, default 24). Each
           window is read from InfluxDB, pivoted to one row per observation,
           inserted, then re-read from ClickHouse and compared. Progress is
           checkpointed to a state file, so an interrupted run resumes where
           it left off.
  verify   Re-runs only the comparison step over a range, without writing.

Examples
  python3 migrate_influx_to_clickhouse.py inspect --pipeline tempest
  python3 migrate_influx_to_clickhouse.py migrate --pipeline tempest
  python3 migrate_influx_to_clickhouse.py migrate --pipeline owm --start 2025-01-01 --dry-run
  python3 migrate_influx_to_clickhouse.py verify  --pipeline tempest

Configuration (environment, same names the pollers use)
  INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG            - source (token needs read on the bucket)
  CLICKHOUSE_HOST/PORT/USER/PASSWORD/SECURE       - destination (user needs CREATE,
                                                    INSERT and SELECT on the database)
  CLICKHOUSE_DATABASE                             - overrides the pipeline's default database

How it maps data
  * Measurement -> table of the same name (tempest_observations, ...).
  * Tags -> string columns ('' when a point didn't have the tag).
  * Fields -> the column of the same name. Numeric fields with no column of
    their own (observations tables only) go into the `extra` map; anything
    else is counted and reported as dropped.
  * Old schema quirks are absorbed automatically: pressure_trend (Tempest
    < v1.9) and weather_main/weather_description (OWM < v1.4) were tags back
    then and fields later - either way they land in their string column.
  * Points that only differed by tag values InfluxDB treated as separate
    series (e.g. a station renamed, or the old pressure_trend tag) but share
    the ClickHouse identity (station/location + second) are merged into one
    row, later non-empty values winning. These are counted as "merged".
  * Timestamps are truncated to whole seconds (the pollers only ever wrote
    second precision).
  * Values are copied exactly as stored, with one opt-in exception: Tempest
    poller < v1.7 stored raw metric values. `inspect` finds where the
    metric -> imperial switch happened (station_pressure in hPa vs inHg),
    and `--tempest-metric-before WHEN` converts older tempest_observations
    rows with the poller's own formulas. Every row's pressure is checked
    against the cutoff, so a wrong cutoff stops the run instead of
    double-converting or leaving metric rows behind.
  * The *_poller_healthcheck measurements are not migrated.

Running it alongside the new pollers is safe: the destination tables are
ReplacingMergeTree, so an observation both written live and migrated ends up
as a single row.
"""

import argparse
import json
import logging
import math
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone

import clickhouse_schema as chs

log = logging.getLogger("influx-migrate")

INFLUX_META_COLUMNS = {"result", "table", "_start", "_stop", "_measurement", "_time", "_field", "_value"}
EPOCH0 = "1970-01-01T00:00:00Z"
MAX_EXAMPLES = 5


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in tests/test_migrate.py)
# ---------------------------------------------------------------------------


def parse_time(value):
    """Accepts epoch seconds, YYYY-MM-DD or RFC3339; returns epoch seconds (int, UTC)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    v = value.strip()
    if v.isdigit():
        return int(v)
    if len(v) == 10:
        v += "T00:00:00Z"
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(math.floor(dt.timestamp()))


def rfc3339(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_epoch_seconds(t):
    """InfluxDB returns tz-aware datetimes (possibly with sub-second parts)."""
    if isinstance(t, datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return int(math.floor(t.timestamp()))
    return int(t)


def iter_chunks(start, stop, chunk_seconds):
    s = start
    while s < stop:
        e = min(s + chunk_seconds, stop)
        yield s, e
        s = e


def flux_string(value):
    # Flux string literals use the same escaping as JSON for our purposes
    return json.dumps(value)


def is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def coerce(value, ch_type):
    """Convert an InfluxDB value to what the ClickHouse column wants, or raise ValueError."""
    if ch_type.startswith("Nullable(Float") or ch_type.startswith("Float"):
        if is_number(value):
            return float(value)
        raise ValueError("expected number")
    if ch_type.startswith(("UInt", "Int")):
        if is_number(value) or isinstance(value, bool):
            return int(value)
        raise ValueError("expected integer")
    if "String" in ch_type:
        return value if isinstance(value, str) else str(value)
    raise ValueError(f"unsupported column type {ch_type}")


class MappingStats:
    def __init__(self):
        self.records = 0  # pivoted rows read from InfluxDB
        self.merged = 0  # rows folded into another row with the same ClickHouse key
        self.to_extra = Counter()  # key -> count routed into the `extra` map
        self.dropped = Counter()  # key -> count of values with nowhere to go

    def add(self, other):
        self.records += other.records
        self.merged += other.merged
        self.to_extra.update(other.to_extra)
        self.dropped.update(other.dropped)


def record_to_row(table, values, stats):
    """Map one pivoted InfluxDB record (dict) to a ClickHouse row dict."""
    types = dict(table.columns)
    has_extra = "extra" in types
    row = {"time": to_epoch_seconds(values["_time"])}
    extra = {}

    for key, value in values.items():
        if key in INFLUX_META_COLUMNS or value is None or key in ("time", "extra", "inserted_at"):
            continue
        if key in types:
            try:
                row[key] = coerce(value, types[key])
            except ValueError:
                stats.dropped[key] += 1
        elif has_extra and is_number(value):
            extra[key] = float(value)
            stats.to_extra[key] += 1
        else:
            stats.dropped[key] += 1

    if has_extra:
        row["extra"] = extra
    return row


def row_key(table, row):
    return tuple(row.get(k) if row.get(k) is not None else chs.empty_value(dict(table.columns)[k]) for k in table.key)


def merge_rows(table, rows, stats):
    """Collapse rows sharing a ClickHouse identity; later non-empty values win."""
    merged = {}
    for row in rows:
        k = row_key(table, row)
        if k not in merged:
            merged[k] = row
            continue
        stats.merged += 1
        target = merged[k]
        for col, val in row.items():
            if col == "extra":
                target.setdefault("extra", {}).update(val)
            elif val is not None and val != "":
                target[col] = val
    return merged


# Tempest poller < v1.7 stored raw metric values. Pressure tells the two eras
# apart unambiguously: hPa readings are ~500-1100, inHg readings ~15-32.
PRESSURE_FIELDS = ("station_pressure", "barometric_pressure", "sea_level_pressure")
METRIC_PRESSURE_THRESHOLD = 100.0


class UnitConversionError(Exception):
    pass


def pressure_looks_metric(row):
    """True (hPa), False (inHg), or None when the row has no pressure reading."""
    for f in PRESSURE_FIELDS:
        v = row.get(f)
        if v is not None:
            return v > METRIC_PRESSURE_THRESHOLD
    return None


class TempestUnits:
    """
    Per-row unit handling for tempest_observations.

    With `metric_before` (epoch seconds) set, rows older than it are converted
    from metric to imperial using the poller's own converters, and the cutoff
    is sanity-checked against each row's pressure reading: a pre-cutoff row
    that's already imperial (cutoff too late - would be double-converted) or a
    post-cutoff row that's still metric (cutoff too early - would stay metric)
    raises UnitConversionError.

    Without it, rows are left alone and metric-looking ones are only counted,
    so the summary can point at the option.
    """

    def __init__(self, metric_before=None):
        from tempest_poller import FIELD_UNIT_CONVERTERS  # single source of truth for the formulas

        self.converters = FIELD_UNIT_CONVERTERS
        self.metric_before = metric_before
        self.converted = 0
        self.metric_looking = 0

    def apply(self, row):
        metric = pressure_looks_metric(row)
        where = f"station {row.get('station_id')!r} at {rfc3339(row['time'])}"
        if self.metric_before is None:
            if metric:
                self.metric_looking += 1
            return row
        if row["time"] < self.metric_before:
            if metric is False:
                raise UnitConversionError(
                    f"{where} is before the --tempest-metric-before cutoff but its pressure is already "
                    "imperial (inHg) - the cutoff is too late and this row would be converted twice")
            for field, convert in self.converters.items():
                if row.get(field) is not None:
                    row[field] = convert(row[field])
            self.converted += 1
        elif metric:
            raise UnitConversionError(
                f"{where} is after the --tempest-metric-before cutoff but its pressure is still metric "
                "(hPa) - the cutoff is too early and this row would stay metric")
        return row


def values_equal(a, b):
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is None and b is None
        if math.isnan(a) and math.isnan(b):
            return True
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(a, dict) or isinstance(b, dict):
        a, b = a or {}, b or {}
        return a.keys() == b.keys() and all(values_equal(float(a[k]), float(b[k])) for k in a)
    return a == b


def compare_rows(table, influx_rows, ch_rows):
    """
    influx_rows / ch_rows: {key: row-dict}. Returns a dict with counts and a
    few examples of missing / mismatched keys.
    """
    result = {"checked": 0, "missing": 0, "mismatched": 0, "extra_in_clickhouse": 0, "examples": []}
    names = table.column_names
    for key, irow in influx_rows.items():
        result["checked"] += 1
        crow = ch_rows.get(key)
        if crow is None:
            result["missing"] += 1
            if len(result["examples"]) < MAX_EXAMPLES:
                result["examples"].append(f"missing in ClickHouse: {key}")
            continue
        expected = dict(zip(names, table.row_values(irow)))
        diffs = [c for c in names if not values_equal(expected[c], crow.get(c))]
        if diffs:
            result["mismatched"] += 1
            if len(result["examples"]) < MAX_EXAMPLES:
                detail = ", ".join(f"{c}: influx={expected[c]!r} clickhouse={crow.get(c)!r}" for c in diffs[:4])
                result["examples"].append(f"mismatch at {key}: {detail}")
    result["extra_in_clickhouse"] = len(set(ch_rows) - set(influx_rows))
    return result


# ---------------------------------------------------------------------------
# InfluxDB side
# ---------------------------------------------------------------------------


class InfluxSource:
    def __init__(self, url, token, org, bucket, timeout_ms=600_000):
        from influxdb_client import InfluxDBClient  # only the migration needs this dependency

        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout_ms)
        self.query_api = self.client.query_api()
        self.org = org
        self.bucket = bucket
        self.url = url

    def close(self):
        self.client.close()

    def _query(self, flux):
        return self.query_api.query(flux, org=self.org)

    def ping(self):
        return self.client.ping()

    def measurements(self):
        flux = (
            'import "influxdata/influxdb/schema"\n'
            f"schema.measurements(bucket: {flux_string(self.bucket)}, start: {EPOCH0})"
        )
        return sorted(r.get_value() for t in self._query(flux) for r in t.records)

    def keys(self, measurement, kind):
        fn = "measurementFieldKeys" if kind == "field" else "measurementTagKeys"
        flux = (
            'import "influxdata/influxdb/schema"\n'
            f"schema.{fn}(bucket: {flux_string(self.bucket)}, measurement: {flux_string(measurement)}, start: {EPOCH0})"
        )
        keys = {r.get_value() for t in self._query(flux) for r in t.records}
        return sorted(k for k in keys if k not in INFLUX_META_COLUMNS)

    def time_bound(self, measurement, which, where=None):
        """
        Earliest ('first') or latest ('last') point time for a measurement, or
        None. `where` is an extra Flux predicate on r, e.g.
        'r._field == "station_pressure" and r._value > 100.0'.
        """
        desc = "true" if which == "last" else "false"
        extra = f"  |> filter(fn: (r) => {where})\n" if where else ""
        flux = (
            f"from(bucket: {flux_string(self.bucket)})\n"
            f"  |> range(start: {EPOCH0})\n"
            f"  |> filter(fn: (r) => r._measurement == {flux_string(measurement)})\n"
            f"{extra}"
            f"  |> {which}()\n"
            '  |> keep(columns: ["_time"])\n'
            "  |> group()\n"
            f'  |> sort(columns: ["_time"], desc: {desc})\n'
            "  |> limit(n: 1)"
        )
        for t in self._query(flux):
            for r in t.records:
                return to_epoch_seconds(r.get_time())
        return None

    def pivoted_records(self, measurement, start, stop):
        """One dict per (series, timestamp) with every field as its own key."""
        flux = (
            f"from(bucket: {flux_string(self.bucket)})\n"
            f"  |> range(start: {rfc3339(start)}, stop: {rfc3339(stop)})\n"
            f"  |> filter(fn: (r) => r._measurement == {flux_string(measurement)})\n"
            '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
        )
        for record in self.query_api.query_stream(flux, org=self.org):
            yield record.values


def read_chunk(source, table, start, stop, stats, units=None):
    rows = []
    for values in source.pivoted_records(table.influx_measurement, start, stop):
        stats.records += 1
        row = record_to_row(table, values, stats)
        rows.append(units.apply(row) if units else row)
    return merge_rows(table, rows, stats)


# ---------------------------------------------------------------------------
# ClickHouse side
# ---------------------------------------------------------------------------


def fetch_clickhouse_rows(client, database, table, start, stop):
    cols = [c for c in table.column_names if c != "time"]
    select = ", ".join(["toUnixTimestamp(`time`) AS `time`"] + [chs.quote_ident(c) for c in cols])
    sql = (
        f"SELECT {select} FROM {chs.quote_ident(database)}.{chs.quote_ident(table.name)} FINAL "
        "WHERE `time` >= toDateTime({start:UInt32}, 'UTC') AND `time` < toDateTime({stop:UInt32}, 'UTC')"
    )
    result = client.query(sql, parameters={"start": start, "stop": stop})
    names = ["time"] + cols
    rows = {}
    for raw in result.result_rows:
        row = dict(zip(names, raw))
        row["time"] = int(row["time"])
        rows[row_key(table, row)] = row
    return rows


# ---------------------------------------------------------------------------
# State file (resume support)
# ---------------------------------------------------------------------------


class State:
    def __init__(self, path, load=True, save=True):
        self.path = path
        self.enabled = bool(path) and save
        self.data = {}
        if path and load and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self.data = json.load(f)

    @staticmethod
    def key(bucket, measurement, database, table):
        return f"influx:{bucket}/{measurement} -> clickhouse:{database}.{table}"

    def done_until(self, key):
        return self.data.get(key, {}).get("done_until")

    def mark(self, key, stop):
        if not self.enabled or not self.path:
            return
        self.data[key] = {"done_until": stop, "done_until_utc": rfc3339(stop), "updated": rfc3339(int(time.time()))}
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def migratable_tables(pipeline, only=None):
    tables = [t for t in chs.PIPELINES[pipeline]["tables"] if t.influx_measurement]
    if only:
        unknown = set(only) - {t.influx_measurement for t in tables}
        if unknown:
            raise SystemExit(f"unknown measurement(s) for pipeline {pipeline}: {', '.join(sorted(unknown))}")
        tables = [t for t in tables if t.influx_measurement in only]
    return tables


def resolve_range(source, table, args):
    start = parse_time(args.start) if args.start else source.time_bound(table.influx_measurement, "first")
    if start is None:
        return None, None
    stop = parse_time(args.stop) if args.stop else int(time.time()) + 1
    if not args.start:
        start -= start % 86400  # align the first chunk to a UTC day boundary
    return start, stop


def units_report(source):
    """
    Find where tempest_observations switched from metric (poller < v1.7) to
    imperial, using station_pressure (hPa > 100 > inHg). Returns
    (lines, suggested_cutoff_or_None).
    """
    m = TEMPEST_OBSERVATIONS_MEASUREMENT
    metric = f'r._field == "station_pressure" and r._value > {METRIC_PRESSURE_THRESHOLD}'
    imperial = f'r._field == "station_pressure" and r._value < {METRIC_PRESSURE_THRESHOLD}'
    first_metric = source.time_bound(m, "first", metric)
    last_metric = source.time_bound(m, "last", metric)
    first_imperial = source.time_bound(m, "first", imperial)

    if last_metric is None:
        return ["units: all pressure readings are imperial - no conversion needed"], None
    if first_imperial is None:
        return [f"units: ALL readings look metric ({rfc3339(first_metric)} .. {rfc3339(last_metric)}) - "
                "the poller ran with TEMPEST_UNITS=metric or predates v1.7 throughout"], None
    lines = [f"units: metric (hPa) readings {rfc3339(first_metric)} .. {rfc3339(last_metric)}, "
             f"imperial (inHg) from {rfc3339(first_imperial)}"]
    if last_metric < first_imperial:
        lines.append(f"       -> migrate with  --tempest-metric-before {rfc3339(first_imperial)}")
        return lines, first_imperial
    lines.append("       -> metric and imperial readings OVERLAP (e.g. stations upgraded at different times, "
                 "or units toggled back and forth) - a single cutoff can't separate them")
    return lines, None


TEMPEST_OBSERVATIONS_MEASUREMENT = chs.TEMPEST_OBSERVATIONS.influx_measurement


def cmd_inspect(args, source):
    tables = {t.influx_measurement: t for t in migratable_tables(args.pipeline)}
    present = source.measurements()
    print(f"Bucket '{source.bucket}' on {source.url} contains {len(present)} measurement(s)\n")

    for m in present:
        table = tables.get(m)
        if table is None:
            note = "(poller startup checks - not migrated)" if m.endswith("_poller_healthcheck") else "(no matching table - NOT migrated)"
            print(f"- {m} {note}")
            continue
        first = source.time_bound(m, "first")
        last = source.time_bound(m, "last")
        span = f"{rfc3339(first)} .. {rfc3339(last)}" if first is not None else "empty"
        print(f"- {m} -> {args.database}.{table.name}   [{span}]")
        if table is chs.TEMPEST_OBSERVATIONS and first is not None:
            for line in units_report(source)[0]:
                print(f"      {line}")
        types = dict(table.columns)
        for kind in ("tag", "field"):
            for k in source.keys(m, kind):
                if k in types:
                    dest = f"column {k}"
                elif kind == "field" and "extra" in types:
                    dest = f"extra['{k}'] (if numeric)"
                else:
                    dest = "DROPPED (no column)"
                print(f"      {kind:5} {k:40} -> {dest}")
    missing = sorted(set(tables) - set(present))
    if missing:
        print(f"\nExpected but not found in bucket: {', '.join(missing)}")
    return 0


def run_chunks(args, source, client, write):
    """Shared loop for migrate (write=True) and verify (write=False)."""
    # --restart ignores saved progress but still records new progress
    state = State(args.state_file, load=not args.restart, save=write and not args.dry_run)
    chunk_seconds = int(args.chunk_hours * 3600)
    metric_before = getattr(args, "tempest_metric_before", None)
    failures = 0
    summary = []

    for table in migratable_tables(args.pipeline, args.measurements):
        m = table.influx_measurement
        start, stop = resolve_range(source, table, args)
        if start is None:
            log.info("%s: no data in bucket '%s' - skipping", m, source.bucket)
            continue

        units = TempestUnits(metric_before) if table is chs.TEMPEST_OBSERVATIONS else None
        skey = State.key(source.bucket, m, args.database, table.name)
        if units and metric_before is not None:
            # A different cutoff means different stored values - track progress separately
            skey += f" [metric before {rfc3339(metric_before)}]"
        resume_from = state.done_until(skey) if state.enabled else None
        if resume_from and resume_from > start:
            log.info("%s: resuming from %s (state file %s)", m, rfc3339(resume_from), args.state_file)
            start = resume_from
        if start >= stop:
            log.info("%s: already migrated up to %s - nothing to do", m, rfc3339(stop))
            continue

        log.info("%s -> %s.%s: %s .. %s in %sh chunks", m, args.database, table.name,
                 rfc3339(start), rfc3339(stop), args.chunk_hours)
        stats = MappingStats()
        totals = Counter()
        t0 = time.time()

        for c_start, c_stop in iter_chunks(start, stop, chunk_seconds):
            chunk_stats = MappingStats()
            try:
                rows = read_chunk(source, table, c_start, c_stop, chunk_stats, units)
            except UnitConversionError as e:
                log.error("%s: %s", m, e)
                log.error("Run `inspect` to find the right --tempest-metric-before value, then re-run with "
                          "--restart (rows already copied are overwritten, not duplicated).")
                return 1, summary
            stats.add(chunk_stats)
            totals["rows"] += len(rows)

            if write and not args.dry_run and rows:
                chs.insert_rows(client, args.database, table, list(rows.values()))
                totals["inserted"] += len(rows)

            if client is not None and not args.dry_run and (not write or args.verify):
                ch_rows = fetch_clickhouse_rows(client, args.database, table, c_start, c_stop)
                result = compare_rows(table, rows, ch_rows)
                for k in ("checked", "missing", "mismatched", "extra_in_clickhouse"):
                    totals[k] += result[k]
                if result["missing"] or result["mismatched"]:
                    failures += 1
                    log.error("%s %s..%s: %d missing, %d mismatched of %d", m, rfc3339(c_start), rfc3339(c_stop),
                              result["missing"], result["mismatched"], result["checked"])
                    for ex in result["examples"]:
                        log.error("    %s", ex)
                    if write:
                        log.error("Stopping so the problem can be looked at; re-run to resume from this chunk.")
                        return 1, summary

            if write and not args.dry_run:
                state.mark(skey, c_stop)
            if rows:
                log.info("  %s .. %s: %d row(s)%s", rfc3339(c_start), rfc3339(c_stop), len(rows),
                         " (dry run)" if args.dry_run else "")

        if stats.dropped:
            log.warning("%s: values with no destination column were dropped: %s", m, dict(stats.dropped))
        if stats.to_extra:
            log.info("%s: fields stored in the `extra` map: %s", m, dict(stats.to_extra))
        if units and units.metric_looking:
            log.warning("%s: %d row(s) look METRIC (pressure in hPa) and were copied unconverted - "
                        "probably from poller < v1.7. Run `inspect` and consider --tempest-metric-before.",
                        m, units.metric_looking)
        line = (f"{m}: {stats.records} influx rows -> {totals['rows']} clickhouse rows "
                f"({stats.merged} merged)")
        if write and not args.dry_run:
            line += f", {totals['inserted']} inserted"
        if totals["checked"]:
            line += (f", verified {totals['checked']}: {totals['missing']} missing, "
                     f"{totals['mismatched']} mismatched, {totals['extra_in_clickhouse']} only in ClickHouse")
        if units and units.converted:
            line += f", {units.converted} converted metric->imperial"
        if units and units.metric_looking:
            line += f", {units.metric_looking} look metric (unconverted)"
        line += f" [{time.time() - t0:.0f}s]"
        summary.append(line)
        log.info(line)

    return (1 if failures else 0), summary


def build_parser():
    p = argparse.ArgumentParser(
        description="Migrate the weather stack's InfluxDB 2.x history into ClickHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp):
        sp.add_argument("--pipeline", choices=sorted(chs.PIPELINES), required=True)
        sp.add_argument("--bucket", help="InfluxDB bucket (default: tempest / weathermap)")
        sp.add_argument("--database", help="ClickHouse database (default: $CLICKHOUSE_DATABASE, else tempest / weathermap)")
        sp.add_argument("--influx-url", default=os.environ.get("INFLUX_URL", "http://localhost:8086"))
        sp.add_argument("--influx-org", default=os.environ.get("INFLUX_ORG"))
        sp.add_argument("--influx-timeout", type=int, default=600, help="InfluxDB query timeout, seconds (default 600)")
        sp.add_argument("-v", "--verbose", action="store_true")

    sp = sub.add_parser("inspect", help="show what's in the bucket and where each key will land")
    common(sp)

    for name, helptext in (("migrate", "copy data, verifying each chunk"), ("verify", "compare only, no writes")):
        sp = sub.add_parser(name, help=helptext)
        common(sp)
        sp.add_argument("--measurements", nargs="+", help="only these measurements (default: all for the pipeline)")
        sp.add_argument("--start", help="UTC start: YYYY-MM-DD, RFC3339 or epoch (default: earliest point)")
        sp.add_argument("--stop", help="UTC stop, exclusive (default: now)")
        sp.add_argument("--chunk-hours", type=float, default=24.0, help="window size per read/insert (default 24)")
        sp.add_argument(
            "--tempest-metric-before", metavar="WHEN",
            help="convert tempest_observations rows older than this from metric to imperial (history "
                 "from Tempest poller < v1.7). `inspect` suggests the value. Pass the same value to verify.")
        if name == "migrate":
            sp.add_argument("--dry-run", action="store_true", help="read and map only; don't touch ClickHouse")
            sp.add_argument("--no-verify", dest="verify", action="store_false", help="skip the per-chunk comparison")
            sp.add_argument("--state-file", help="progress file (default: migration_state_<pipeline>.json)")
            sp.add_argument("--restart", action="store_true", help="ignore saved progress and start over")
            sp.add_argument("--no-create-schema", dest="create_schema", action="store_false",
                            help="don't run CREATE ... IF NOT EXISTS first")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    for noisy in ("urllib3", "clickhouse_connect", "influxdb_client"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    pipe = chs.PIPELINES[args.pipeline]
    args.bucket = args.bucket or pipe["default_bucket"]
    args.database = chs.validate_ident(
        args.database or os.environ.get("CLICKHOUSE_DATABASE") or pipe["default_database"]
    )
    if getattr(args, "tempest_metric_before", None):
        if args.pipeline != "tempest":
            log.error("--tempest-metric-before only applies to --pipeline tempest")
            return 2
        args.tempest_metric_before = parse_time(args.tempest_metric_before)
    if args.command == "verify":
        args.dry_run, args.verify, args.restart, args.state_file = False, True, False, None
    elif args.command == "migrate" and not args.state_file:
        args.state_file = f"migration_state_{args.pipeline}.json"

    token = os.environ.get("INFLUX_TOKEN")
    if not token or not args.influx_org:
        log.error("Set INFLUX_TOKEN and INFLUX_ORG (or --influx-org) for the source InfluxDB")
        return 2

    source = InfluxSource(args.influx_url, token, args.influx_org, args.bucket, timeout_ms=args.influx_timeout * 1000)
    try:
        if not source.ping():
            log.error("InfluxDB at %s did not respond to ping", args.influx_url)
            return 2

        if args.command == "inspect":
            return cmd_inspect(args, source)

        client = None
        if not getattr(args, "dry_run", False):
            settings = chs.clickhouse_settings_from_env()
            client = chs.get_client(settings)
            log.info("Destination: %s database '%s'", chs.describe(settings), args.database)
            if args.command == "migrate" and args.create_schema:
                chs.ensure_schema(client, args.pipeline, args.database)

        code, summary = run_chunks(args, source, client, write=(args.command == "migrate"))
        print("\nSummary:")
        for line in summary or ["(nothing to do)"]:
            print(f"  {line}")
        if code:
            print("\nFAILED - see errors above.")
        elif args.command == "verify" or (args.command == "migrate" and args.verify and not args.dry_run):
            print("\nAll migrated rows verified against InfluxDB.")
        return code
    finally:
        source.close()


if __name__ == "__main__":
    sys.exit(main())
