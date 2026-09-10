# Tempest → InfluxDB → Grafana Setup

## Overview
- **Poller + InfluxDB 2.x**: can run together on one host (container or VM), or split across two if you prefer.
- **Grafana**: can be the same host, or a separate one — this guide assumes it's already installed somewhere reachable over the network.

## System requirements

**Poller host**
- Linux (any modern distro with systemd - Ubuntu, Debian, etc.); this guide's commands assume Debian/Ubuntu-based
- Python 3.8+
- ~50 MB disk for the poller itself; negligible CPU/RAM (a single-station poll cycle is lightweight)
- Outbound HTTPS access to:
  - `swd.weatherflow.com` (Tempest API)
  - `api.weather.gov` (NWS alerts, if enabled)
  - `repos.influxdata.com` (only during InfluxDB install)

**InfluxDB 2.x**
- Same host as the poller, or a separate one reachable over the network
- ~1 GB disk minimum for a small/personal deployment; grows with retention period and station count
- Port `8086` reachable from wherever Grafana runs

**Grafana**
- Any version supporting the InfluxDB (Flux) data source (Grafana 8+ recommended)
- Network access to the InfluxDB host's port `8086`
- No specific OS requirement — this guide doesn't cover Grafana's own installation, only configuring the data source and importing the dashboard

**Accounts/credentials needed before starting**
- A WeatherFlow Tempest account with at least one registered station
- (Optional) enough familiarity with `sudo`/systemd to install services

---

## 1. Install InfluxDB 2.x on the poller host

```bash
# On Debian/Ubuntu-based systems.
# Note: InfluxData rotated their package signing key in January 2026 - the
# older "influxdata-archive_compat.key" some guides reference is expired.
# This uses their current key and verifies its fingerprint before trusting it.
curl --silent --location -O https://repos.influxdata.com/influxdata-archive.key

gpg --show-keys --with-fingerprint --with-colons ./influxdata-archive.key 2>&1 \
  | grep -q '^fpr:\+24C975CBA61A024EE1B631787C3D57159FC2F927:$' \
  && echo "Fingerprint OK" || echo "FINGERPRINT MISMATCH - DO NOT PROCEED"

# Only continue if the above printed "Fingerprint OK"
sudo mkdir -p /etc/apt/keyrings
cat influxdata-archive.key | gpg --dearmor | sudo tee /etc/apt/keyrings/influxdata-archive.gpg > /dev/null
echo 'deb [signed-by=/etc/apt/keyrings/influxdata-archive.gpg] https://repos.influxdata.com/debian stable main' | sudo tee /etc/apt/sources.list.d/influxdata.list

sudo apt update
sudo apt install -y influxdb2
sudo systemctl enable --now influxdb
```


Run initial setup (creates your org, bucket, admin user, and initial token):

```bash
influx setup \
  --username admin \
  --org home \
  --bucket tempest \
  --retention 0 \
  --force
```

Note the token it prints, or generate a scoped write-only token afterward:

```bash
influx auth create \
  --org home \
  --write-bucket $(influx bucket list --org home --name tempest --hide-headers | cut -f1) \
  --description "tempest-poller"
```

InfluxDB's UI is at `http://<container-ip>:8086` if you want to manage it visually instead.

## 2. Get your Tempest API credentials

1. Log into [tempestwx.com](https://tempestwx.com), go to your account settings.
2. Under "Data Authorizations," create a **Personal Use Token**.
3. Find your **Station ID** — visible in the station settings page URL or via `https://swd.weatherflow.com/swd/rest/stations?token=YOUR_TOKEN`.

> **Viewing someone else's station**: your Personal Use Token only grants access to stations you own. To poll a station owned by someone else, that station's owner must first share it with your Tempest account (from their station's settings on tempestwx.com, under station sharing/permissions) before its Station ID will work with your token.

## 3. Deploy the poller

```bash
sudo useradd -r -s /usr/sbin/nologin tempest
sudo mkdir -p /opt/tempest-poller
sudo cp tempest_poller.py requirements.txt /opt/tempest-poller/
cd /opt/tempest-poller
sudo python3 -m venv venv
sudo ./venv/bin/pip install -r requirements.txt

sudo cp tempest-poller.env.example /etc/tempest-poller.env
sudo chmod 600 /etc/tempest-poller.env
sudo nano /etc/tempest-poller.env   # fill in TEMPEST_TOKEN, TEMPEST_STATION_ID, INFLUX_TOKEN, INFLUX_ORG

sudo chown -R tempest:tempest /opt/tempest-poller

sudo cp tempest-poller.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tempest-poller
sudo systemctl status tempest-poller
journalctl -u tempest-poller -f
```

You should see log lines like `Wrote 1 point(s) to InfluxDB` roughly every 60 seconds.

## 4. Connect Grafana

1. In Grafana: **Connections → Data sources → Add data source → InfluxDB**.
2. Settings:
   - **Query language**: Flux
   - **URL**: `http://<influxdb-host-ip>:8086`
   - **Organization**: `home` (or whatever you named it)
   - **Token**: the write/read token from step 1 (create a read-only one for Grafana if you prefer least-privilege)
   - **Default bucket**: `tempest`
3. Click **Save & test** — it should confirm connectivity.

> Make sure port 8086 on the InfluxDB host is reachable from wherever Grafana runs (same network/bridge, or open the firewall/port if they're on different subnets).

## 5. Import the dashboard

1. In Grafana: **Dashboards → New → Import**.
2. Upload `tempest_dashboard.json`.
3. When prompted, select the InfluxDB datasource you just created.
4. Save.

You'll get panels for temperature, humidity, wind speed/direction, pressure, daily rain accumulation, UV, solar radiation/illuminance, and lightning strikes, plus freeze/wind/heat warnings and National Weather Service severe alerts.

---

## Notes
- Tempest stations report roughly every **60 seconds**, so `POLL_INTERVAL=60` matches that; polling faster won't get you new data.
- `precip` is the rain amount since the *last report*, not a running daily total — the dashboard sums it with a daily `aggregateWindow`.
- **Units**: the Tempest REST API always returns raw metric values (Celsius, m/s, hPa, mm) internally, regardless of what the `station_units` field in the API response claims — that field only reflects a display preference in WeatherFlow's own app, not the actual format of the data. The poller's `TEMPEST_UNITS` setting (default `imperial`) converts these raw metric values to °F/mph/inHg/inches at write time; set it to `metric` if you'd rather store the raw values unconverted.
- **Multiple stations**: set `TEMPEST_STATION_IDS=<station_id_1>,<station_id_2>` (comma-separated, using your own station IDs — find these under your account's station settings on tempestwx.com) in the env file. The poller polls each one every cycle and tags every point with `station_id`, `station_name`, and (if you've set one) `location_name`, so Grafana's `$station_id` dropdown variable can filter or show all of them together.
- **Location names**: `location_name` comes only from `STATION_LOCATION_OVERRIDES` — a manual `station_id:Location Name` map you set yourself. There's no automatic geocoding; a station with no entry in that map simply has no `location_name` tag.
- If you'd rather avoid the cloud API entirely, the Tempest hub also broadcasts observations via local UDP on your network — that's a different poller design (listening for broadcasts instead of making HTTP requests) not covered by this guide.
