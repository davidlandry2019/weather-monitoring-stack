# Tempest → ClickHouse → Grafana Setup

> **Coming from the InfluxDB version of this stack?** Set up ClickHouse and the
> new pollers using this guide, then follow [`MIGRATION.md`](MIGRATION.md)
> to copy your history across.

## Overview
- **Poller + ClickHouse**: can run together on one host (container or VM), or split across two if you prefer.
- **Grafana**: can be the same host, or a separate one — this guide assumes it's already installed somewhere reachable over the network.

## System requirements

**Poller host**
- Linux (any modern distro with systemd - Ubuntu, Debian, etc.); this guide's commands assume Debian/Ubuntu-based
- Python 3.9+ (the installer installs it if needed)
- ~50 MB disk for the poller itself; negligible CPU/RAM (a single-station poll cycle is lightweight)
- Outbound HTTPS access to:
  - `swd.weatherflow.com` (Tempest API)
  - `api.weather.gov` (NWS alerts, if enabled)
  - `packages.clickhouse.com` (only during ClickHouse install)

**ClickHouse**
- Same host as the poller, or a separate one reachable over the network
- ~1 GB disk is plenty for a small/personal deployment — ClickHouse compresses weather data very well (a station's year of 1-minute observations is typically tens of MB)
- 2 GB RAM recommended for the server
- HTTP port `8123` (or `8443` with TLS) reachable from the poller host and from wherever Grafana runs

**Grafana**
- Grafana 10+ with the [ClickHouse data source plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) (`grafana-clickhouse-datasource`, v4+)
- Network access to the ClickHouse host's HTTP port
- No specific OS requirement — this guide doesn't cover Grafana's own installation, only configuring the data source and importing the dashboard

**Accounts/credentials needed before starting**
- A WeatherFlow Tempest account with at least one registered station
- (Optional) enough familiarity with `sudo`/systemd to install services

---

## 1. Install ClickHouse

Official DEB repository (see [clickhouse.com/docs/install/debian_ubuntu](https://clickhouse.com/docs/install/debian_ubuntu)):

```bash
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
  | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

ARCH=$(dpkg --print-architecture)
echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${ARCH}] https://packages.clickhouse.com/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/clickhouse.list

sudo apt-get update
sudo apt-get install -y clickhouse-server clickhouse-client   # prompts for a password for the `default` user - set one
sudo systemctl enable --now clickhouse-server
```

By default ClickHouse only listens on localhost. If Grafana or a poller runs on
a different host, let it listen on the network:

```bash
sudo tee /etc/clickhouse-server/config.d/listen.xml > /dev/null <<'EOF'
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
</clickhouse>
EOF
sudo systemctl restart clickhouse-server
```

## 2. Create the database, tables and users

Create the schema (this is the same thing the poller does on startup when
`CLICKHOUSE_AUTO_CREATE_SCHEMA=true`; doing it up front lets the poller run with
an insert-only user):

```bash
# from a checkout of this repo, with clickhouse-connect installed (pip install -r requirements.txt)
CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD='<default password>' \
  python3 clickhouse_schema.py create --pipeline tempest
# or, without Python:  python3 clickhouse_schema.py print --pipeline tempest | clickhouse-client --password --multiquery
```

Then create a write-only user for the poller and a read-only one for Grafana
(and anything else that only reads, such as other dashboards):

```bash
clickhouse-client --password
```
```sql
CREATE USER tempest_poller IDENTIFIED WITH sha256_password BY 'choose-a-password';
GRANT INSERT ON tempest.* TO tempest_poller;

CREATE USER weather_reader IDENTIFIED WITH sha256_password BY 'choose-another-password';
GRANT SELECT ON tempest.* TO weather_reader;
```

> If `CREATE USER` fails with "Not enough privileges", SQL user management
> isn't enabled for `default` yet. Enable it with
> `/etc/clickhouse-server/users.d/default-access.xml`:
> `<clickhouse><users><default><access_management>1</access_management></default></users></clickhouse>`,
> then `sudo systemctl restart clickhouse-server` and retry.

If you'd rather let the poller create its own tables, also grant
`GRANT CREATE DATABASE, CREATE TABLE ON tempest.* TO tempest_poller;` and leave
`CLICKHOUSE_AUTO_CREATE_SCHEMA=true`.

**One database or two?** The default is separate `tempest` and `weathermap`
databases (mirroring the old separate InfluxDB buckets). Table names are
prefixed (`tempest_*`, `owm_*`), so pointing both pollers at one shared
database works too — set `CLICKHOUSE_DATABASE` to the same name in both env
files and enter that name when importing each dashboard.

## 3. Get your Tempest API credentials

1. Log into [tempestwx.com](https://tempestwx.com), go to your account settings.
2. Under "Data Authorizations," create a **Personal Use Token**.
3. Find your **Station ID** — visible in the station settings page URL or via `https://swd.weatherflow.com/swd/rest/stations?token=YOUR_TOKEN`.

> **Viewing someone else's station**: your Personal Use Token only grants access to stations you own. To poll a station owned by someone else, that station's owner must first share it with your Tempest account (from their station's settings on tempestwx.com, under station sharing/permissions) before its Station ID will work with your token.

## 4. Install the pollers

On the poller host (Debian/Ubuntu with systemd):

```bash
sudo git clone https://github.com/davidlandry2019/weather-monitoring-stack.git /opt/weather-monitoring-stack
sudo bash /opt/weather-monitoring-stack/deploy/install.sh
```

The installer sets up Python, the `.venv` and the `weather-poller` service user,
and runs the tests. It then creates `tempest-poller.env` and `owm-poller.env` in
`/opt/weather-monitoring-stack` (owned `root:weather-poller`, mode 640). It asks
for each poller's ClickHouse password if it's run in a terminal. A poller only
starts once its env file is filled in, so the first run usually ends with
"waiting for configuration". Edit the file:

```bash
sudo nano /opt/weather-monitoring-stack/tempest-poller.env
```

Fill in:
- `TEMPEST_TOKEN`, `TEMPEST_STATION_IDS`
- `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` (and `CLICKHOUSE_SECURE=true` if you put TLS in front)
- `CLICKHOUSE_USER=tempest_poller`, `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_AUTO_CREATE_SCHEMA=false` if you created the schema in step 2 and only granted INSERT
- `NWS_USER_AGENT` - identify your app/contact per NWS's API policy

Then run the installer again. It installs and starts `tempest-poller.service`
and waits for the poller to log `Startup checks passed`:

```bash
sudo bash /opt/weather-monitoring-stack/deploy/install.sh
journalctl -u tempest-poller -f        # expect "Wrote N row(s) to ClickHouse" every minute
```

You can also pass the ClickHouse settings to the installer instead of editing
the file: `sudo CLICKHOUSE_HOST=clickhouse.lan TEMPEST_CLICKHOUSE_PASSWORD='...' bash .../install.sh`.
Only install one poller with `POLLERS=tempest` (or `POLLERS=owm`).

**Updating:** run the same `install.sh` command. It pulls the latest code first,
runs the tests, and only restarts the pollers if they pass. Your env files are
never overwritten.

Check data is landing:

```sql
SELECT station_name, max(time), count() FROM tempest.tempest_observations GROUP BY station_name;
```

## 5. Connect Grafana

1. Install the plugin: **Administration → Plugins → ClickHouse → Install** (or `grafana-cli plugins install grafana-clickhouse-datasource`).
2. **Connections → Data sources → Add data source → ClickHouse**.
3. Settings:
   - **Server address**: the ClickHouse host; **Protocol**: HTTP; **Server port**: `8123` (or `8443` with **Secure connection** on)
   - **Username / Password**: `weather_reader` and its password from step 2
   - **Default database**: `tempest` (optional - the dashboard names its database explicitly)
4. Click **Save & test** — it should confirm connectivity.

> Make sure ClickHouse's HTTP port is reachable from wherever Grafana runs (`listen_host` from step 1, plus any firewall between subnets).

## 6. Import the dashboard

1. In Grafana: **Dashboards → New → Import**.
2. Upload `tempest_dashboard.json`.
3. When prompted, pick the ClickHouse data source you just created, and confirm the **ClickHouse database** name (`tempest` unless you changed `CLICKHOUSE_DATABASE`).
4. Save.

You'll get panels for temperature, humidity, wind speed/direction, pressure, daily rain accumulation, UV, solar radiation/illuminance, and lightning strikes, plus freeze/wind/heat warnings and National Weather Service severe alerts.

---

## Data model

One wide row per observation in `tempest.tempest_observations`, ordered by
`(station_id, time)`. `station_name` and `location_name` sit on every row. Field
names match the Tempest API's (`air_temperature`, `wind_gust`, `precip`, ...),
and any numeric field the API adds that has no column yet is kept in the
`extra` map column (`extra['new_field']`). NWS alerts go to `tempest_nws_alerts`
(one row per active alert per NWS poll, `time` = when it was seen) and
`tempest_nws_alert_summary`. `python3 clickhouse_schema.py print --pipeline tempest`
prints the full DDL.

Tables are `ReplacingMergeTree`: re-writing the same observation (the poller
re-reading an unchanged latest obs, or a re-run migration) collapses to one
row. Add `FINAL` to queries that sum or count (`FROM tempest.tempest_observations FINAL`) so
not-yet-merged duplicates are never double-counted — the dashboard does this.

Data is kept forever by default. To expire old rows, e.g. after two years:
`ALTER TABLE tempest.tempest_observations MODIFY TTL time + INTERVAL 2 YEAR;`

## Notes
- Tempest stations report roughly every **60 seconds**, so `POLL_INTERVAL=60` matches that; polling faster won't get you new data.
- `precip` is the rain amount since the *last report*, not a running daily total — the dashboard sums it per local day. The day boundary is `America/Chicago`; edit the timezone in the "Rain Accumulation (daily)" panel query if your stations are elsewhere.
- **Units**: the Tempest REST API always returns raw metric values (Celsius, m/s, hPa, mm) internally, regardless of what the `station_units` field in the API response claims — that field only reflects a display preference in WeatherFlow's own app, not the actual format of the data. The poller's `TEMPEST_UNITS` setting (default `imperial`) converts these raw metric values to °F/mph/inHg/inches/miles at write time; set it to `metric` if you'd rather store the raw values unconverted (the dashboard's unit labels and feels-like formula assume imperial).
- **Multiple stations**: set `TEMPEST_STATION_IDS=<station_id_1>,<station_id_2>` (comma-separated, using your own station IDs — find these under your account's station settings on tempestwx.com) in the env file. The poller polls each one every cycle and stores `station_id`, `station_name`, and (if you've set one) `location_name` on every row, so Grafana's Station dropdown can filter or show all of them together.
- **Location names**: `location_name` comes only from `STATION_LOCATION_OVERRIDES` — a manual `station_id:Location Name` map you set yourself. There's no automatic geocoding; a station with no entry in that map simply has an empty `location_name`.
- If you'd rather avoid the cloud API entirely, the Tempest hub also broadcasts observations via local UDP on your network — that's a different poller design (listening for broadcasts instead of making HTTP requests) not covered by this guide.
