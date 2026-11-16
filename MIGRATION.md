# Migrating from InfluxDB to ClickHouse

Versions of this stack before poller v2.0 stored data in InfluxDB 2.x and drew
the dashboards with Flux. InfluxData has put Flux in
[maintenance mode](https://docs.influxdata.com/flux/v0/future-of-flux/) (no new
features, and InfluxDB 3 doesn't support it), so the stack now uses ClickHouse
and SQL. InfluxDB 2.x itself is still supported, so there's no hard deadline.
Migrate when it suits you.

`migrate_influx_to_clickhouse.py` copies your history across and checks every
row it copies.

## What gets copied where

| InfluxDB (bucket / measurement)          | ClickHouse (database.table)             |
|------------------------------------------|-----------------------------------------|
| `tempest` / `tempest_observations`       | `tempest.tempest_observations`          |
| `tempest` / `tempest_nws_alerts`         | `tempest.tempest_nws_alerts`            |
| `tempest` / `tempest_nws_alert_summary`  | `tempest.tempest_nws_alert_summary`     |
| `weathermap` / `owm_observations`        | `weathermap.owm_observations`           |
| `weathermap` / `owm_nws_alerts`          | `weathermap.owm_nws_alerts`             |
| `weathermap` / `owm_nws_alert_summary`   | `weathermap.owm_nws_alert_summary`      |
| `*_poller_healthcheck`                   | not copied                              |

Bucket and database names are overridable (`--bucket`, `--database`), so you
can also merge both buckets into one shared database.

- **Tags** become string columns. A tag missing on a point becomes `''`.
- **Fields** go into the column with the same name. Numeric fields without a
  column of their own go into the observations tables' `extra` map instead of
  being dropped.
- **Old schema quirks** need no action. Before Tempest poller v1.9, `pressure_trend`
  was a tag, and before OWM poller v1.4, `weather_main`/`weather_description` were too.
  Both old and new forms land in the same column. Influx series that the bug split apart are
  merged back into one row per station (or location) per second.
- **Values are copied as stored, except for old metric Tempest data, which you
  can opt to convert.** Tempest pollers before v1.7 stored raw metric values (°C, m/s, hPa, mm, km).
  The dashboards assume imperial, so that history would display wrongly. See
  [Converting pre-v1.7 metric history](#converting-pre-v17-metric-history).

## Recommended order

1. **Install ClickHouse and create the schema and users.** Follow steps 1–2 of
   [`SETUP-TEMPEST.md`](SETUP-TEMPEST.md) and step 2 of [`SETUP-OWM.md`](SETUP-OWM.md).
2. **Cut the pollers over.** Update the poller files, `pip install -r requirements.txt`,
   replace the `INFLUX_*` lines in each env file with the `CLICKHOUSE_*` ones from the
   `.env.example` files, copy the updated `.service` files, then
   `systemctl daemon-reload && systemctl restart tempest-poller owm-poller`.
   New data now flows into ClickHouse. InfluxDB stops growing but keeps everything
   it already has.
3. **Run the migration** (below). Running it while the new pollers are writing
   is safe. An observation that is both migrated and written live ends up as a
   single row.
4. **Import the new dashboards** (`tempest_dashboard.json`, `owm_dashboard.json`)
   against a ClickHouse data source and compare them with the old ones.
5. **Retire InfluxDB** when you're satisfied. Keep a backup first (`influx backup`).

## Running the migration

On any machine that can reach both databases (the poller host is fine):

```bash
python3 -m venv venv && ./venv/bin/pip install -r requirements-migrate.txt

# Source: a token with read access to the bucket(s)
export INFLUX_URL=http://localhost:8086 INFLUX_ORG=home INFLUX_TOKEN=...
# Destination: a user with CREATE, INSERT and SELECT on the target database(s) (e.g. default)
export CLICKHOUSE_HOST=localhost CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=...
```

### 1. Inspect (read-only)

```bash
./venv/bin/python3 migrate_influx_to_clickhouse.py inspect --pipeline tempest
./venv/bin/python3 migrate_influx_to_clickhouse.py inspect --pipeline owm
```

This lists each measurement's time span and every tag and field key, along with
where each one will land: a column, the `extra` map, or **DROPPED** (a non-numeric
field with no column). Check that nothing you care about shows as dropped.

### 2. Optional dry run

```bash
./venv/bin/python3 migrate_influx_to_clickhouse.py migrate --pipeline tempest --dry-run
```

This reads and maps every window and reports the row counts without touching ClickHouse.

### 3. Migrate

```bash
./venv/bin/python3 migrate_influx_to_clickhouse.py migrate --pipeline tempest
./venv/bin/python3 migrate_influx_to_clickhouse.py migrate --pipeline owm
```

For each measurement, the tool runs from the earliest point to now in 24-hour
windows (`--chunk-hours`). It reads each window from InfluxDB, inserts it, then
reads it back from ClickHouse and compares every row and column. It stops at the
first window that doesn't match exactly.

Progress is saved to `migration_state_<pipeline>.json` after each verified
window. If the run is interrupted or stopped, run the same command again and it
resumes from the last verified window. `--restart` starts over, which is
harmless because re-copied rows collapse onto the existing ones.

Other useful flags:
- `--start 2025-01-01 --stop 2025-07-01` limits the copy to a range (UTC; `--stop` is exclusive).
- `--measurements tempest_observations` copies only some measurements.
- `--database weather` copies into a different, or shared, database.
- `--chunk-hours 6` uses smaller windows if InfluxDB times out on big ones. `--influx-timeout` raises the query timeout.
- `--no-verify` skips the read-back check. This is faster, but `verify` is then worth running afterwards.

At the end, each measurement gets a summary like:

```
tempest_observations: 525600 influx rows -> 525600 clickhouse rows (0 merged), 525600 inserted,
  verified 525600: 0 missing, 0 mismatched, 1440 only in ClickHouse
```

- **merged**: Influx rows that shared a ClickHouse identity (see "Old schema quirks") and were combined.
- **only in ClickHouse**: rows the new pollers wrote that InfluxDB never had. This is expected for the period after step 2.

### Converting pre-v1.7 metric history

`inspect --pipeline tempest` checks `station_pressure` to see which readings
are metric: hPa values are around 1000, inHg values around 30, so the two never
overlap. It then reports, for example:

```
units: metric (hPa) readings 2024-03-02T00:01:00Z .. 2025-01-14T17:42:00Z, imperial (inHg) from 2025-01-14T17:43:00Z
       -> migrate with  --tempest-metric-before 2025-01-14T17:43:00Z
```

Pass that value to `migrate`, and the same value to `verify`, so the check
compares against the converted numbers:

```bash
./venv/bin/python3 migrate_influx_to_clickhouse.py migrate --pipeline tempest --tempest-metric-before 2025-01-14T17:43:00Z
```

Rows older than the cutoff are converted to °F/mph/inHg/in/mi using the poller's
own formulas. Every row is checked against the cutoff, and the run stops on a
mismatch:
- A row before the cutoff whose pressure is already imperial means the cutoff is
  too late. Without the stop, that row would be converted twice.
- A row after the cutoff whose pressure is still metric means the cutoff is too early.

If `inspect` reports that metric and imperial readings **overlap**, no single
cutoff can work. That happens with stations upgraded at different times, or with
units switched back and forth. Migrate with `--measurements` and `--start`/`--stop`
in separate ranges instead, or ask for per-station cutoffs.

Without the flag, metric rows are still copied, unconverted, and the summary
warns how many there were.

### 4. Verify any time

```bash
./venv/bin/python3 migrate_influx_to_clickhouse.py verify --pipeline tempest [--start ... --stop ...]
```

This repeats the full row-by-row comparison without writing anything. The exit
code is non-zero if any row is missing or different.

## Spot checks in ClickHouse

```sql
-- rows per station per month
SELECT station_name, toStartOfMonth(time) AS month, count()
FROM tempest.tempest_observations FINAL
GROUP BY station_name, month ORDER BY station_name, month;

-- fields that arrived without a dedicated column
SELECT DISTINCT arrayJoin(mapKeys(extra)) FROM tempest.tempest_observations;
```

## Grafana

The new dashboards keep the old dashboards' UIDs, so importing one replaces its
Flux predecessor. To compare old and new side by side for a while, change the
UID (and title) in Grafana's import dialog. Panel layout, thresholds and colours
are unchanged; only the data source and queries changed.
