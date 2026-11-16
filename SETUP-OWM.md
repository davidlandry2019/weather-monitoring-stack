# OpenWeatherMap Poller Setup

Mirrors the Tempest poller's structure, but pulls current conditions for
arbitrary cities from OpenWeatherMap's free Current Weather Data API instead
of your own personal station - separate database (by default), separate
service, same ClickHouse server.

This guide assumes ClickHouse is already installed (step 1 of
[`SETUP-TEMPEST.md`](SETUP-TEMPEST.md)). Coming from the InfluxDB version?
See [`MIGRATION.md`](MIGRATION.md) afterwards to bring your history across.

## 1. Get an OpenWeatherMap API key

Sign up at [openweathermap.org](https://openweathermap.org/), then go to
**API keys** in your account and copy your default key (or create a new one).
Free tier: 60 calls/minute, 1,000,000 calls/month - polling a handful of
cities every 5 minutes uses a tiny fraction of that.

## 2. Create the database, tables and users

Kept in its own `weathermap` database by default, since this is unrelated
data (set `CLICKHOUSE_DATABASE` to share one database with the Tempest poller
instead - table names don't collide):

```bash
CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD='<default password>' \
  python3 clickhouse_schema.py create --pipeline owm
```

```bash
clickhouse-client --password
```
```sql
CREATE USER owm_poller IDENTIFIED WITH sha256_password BY 'choose-a-password';
GRANT INSERT ON weathermap.* TO owm_poller;

-- read-only user for Grafana (create it here if you didn't in SETUP-TEMPEST.md)
CREATE USER IF NOT EXISTS weather_reader IDENTIFIED WITH sha256_password BY 'choose-another-password';
GRANT SELECT ON weathermap.* TO weather_reader;
```

To let the poller create its own tables instead, also
`GRANT CREATE DATABASE, CREATE TABLE ON weathermap.* TO owm_poller;` and keep
`CLICKHOUSE_AUTO_CREATE_SCHEMA=true`.

## 3. Deploy the poller

```bash
sudo useradd -r -s /usr/sbin/nologin owm-poller
sudo mkdir -p /opt/scripts/owm-poller
sudo cp owm_poller.py clickhouse_schema.py requirements.txt /opt/scripts/owm-poller/

cd /opt/scripts/owm-poller
sudo python3 -m venv venv
sudo ./venv/bin/pip install -r requirements.txt

sudo cp owm-poller.env.example /opt/scripts/owm-poller/owm-poller.env
sudo chmod 600 /opt/scripts/owm-poller/owm-poller.env
sudo nano /opt/scripts/owm-poller/owm-poller.env
```

Fill in:
- `OWM_API_KEY`
- `OWM_LOCATIONS` - e.g. `Denver,CO,US;Miami,FL,US;Seattle,WA,US`
- `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` (and `CLICKHOUSE_SECURE=true` with TLS)
- `CLICKHOUSE_USER=owm_poller`, `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE=weathermap`
- `CLICKHOUSE_AUTO_CREATE_SCHEMA=false` if the user only has INSERT
- `NWS_USER_AGENT` - identify your app/contact per NWS's API policy

```bash
sudo chown -R owm-poller:owm-poller /opt/scripts/owm-poller
```

## 4. Test manually first

```bash
cd /opt/scripts/owm-poller
sudo -u owm-poller bash -c 'set -a; source owm-poller.env; set +a; ./venv/bin/python3 owm_poller.py'
```

You should see the startup checks pass, then a `Wrote N row(s) to ClickHouse`
confirmation. Ctrl+C once confirmed.

## 5. Install as a systemd service

```bash
sudo cp owm-poller.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now owm-poller
sudo systemctl status owm-poller
journalctl -u owm-poller -f
```

## 6. Grafana

Reuse the ClickHouse data source from the Tempest setup (the `weather_reader`
user can read both databases), or add one as described in step 5 of
`SETUP-TEMPEST.md`. Then import `owm_dashboard.json`, pick that data source, and
confirm the **ClickHouse database** name (`weathermap` by default).

## Notes
- Data lands in `weathermap.owm_observations` (one row per location per OWM
  observation, ordered by `(location, time)`), plus `owm_nws_alerts` and
  `owm_nws_alert_summary`. `python3 clickhouse_schema.py print --pipeline owm`
  prints the full DDL.
- OWM's free tier `q` parameter format is `City,State,Country` for US
  locations - the state code is required to disambiguate cities that share a
  name across states (there are multiple "Springfield"s, etc.).
- Pressure and visibility are always returned by OWM in fixed units (hPa,
  meters) regardless of the `units` parameter - the poller converts these to
  inHg/miles itself when `OWM_UNITS=imperial`.
- Polling more often than OWM refreshes just re-reads the same observation
  (same `dt`); it collapses to a single row rather than piling up duplicates.
- A single location typo (e.g. a misspelled city) won't block the other
  locations from polling - errors are logged per-location.
- National Weather Service severe weather alerts work for any US location
  covered by these coordinates, not just personal weather stations - the
  dashboard's "Active Severe Weather Alerts" panel will be green/empty most
  of the time, which is the expected all-clear state, not a bug.
