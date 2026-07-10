# OpenWeatherMap Poller Setup

Mirrors the Tempest poller's structure, but pulls current conditions for
arbitrary cities from OpenWeatherMap's free Current Weather Data API instead
of your own personal station - separate bucket, separate service, same
InfluxDB instance.

## 1. Get an OpenWeatherMap API key

Sign up at [openweathermap.org](https://openweathermap.org/), then go to
**API keys** in your account and copy your default key (or create a new one).
Free tier: 60 calls/minute, 1,000,000 calls/month - polling a handful of
cities every 5 minutes uses a tiny fraction of that.

## 2. Create a separate InfluxDB bucket + token

Kept separate from the `tempest` bucket since this is unrelated data:

```bash
influx bucket create --name weathermap --org home --retention 0

BUCKET_ID=$(influx bucket list --org home --name weathermap --hide-headers | cut -f1)

influx auth create \
  --org home \
  --write-bucket "$BUCKET_ID" \
  --description "owm-poller"
```

Copy the printed token for the poller's env file.

Also create a **read-only** token for Grafana against this bucket (same
pattern as the Tempest setup):

```bash
influx auth create \
  --org home \
  --read-bucket "$BUCKET_ID" \
  --description "grafana-reader-weathermap"
```

## 3. Deploy the poller

```bash
sudo useradd -r -s /usr/sbin/nologin owm-poller
sudo mkdir -p /opt/scripts/owm-poller
# copy owm_poller.py and requirements (same requirements.txt as the Tempest poller: requests, influxdb-client)

cd /opt/scripts/owm-poller
sudo python3 -m venv venv
sudo ./venv/bin/pip install requests influxdb-client --break-system-packages

sudo cp owm-poller.env.example /opt/scripts/owm-poller/owm-poller.env
sudo chmod 600 /opt/scripts/owm-poller/owm-poller.env
sudo nano /opt/scripts/owm-poller/owm-poller.env
```

Fill in:
- `OWM_API_KEY`
- `OWM_LOCATIONS` - e.g. `Denver,CO,US;Miami,FL,US;Seattle,WA,US`
- `INFLUX_TOKEN` - the `owm-poller` token from step 2
- `INFLUX_ORG=home`
- `INFLUX_BUCKET=weathermap`
- `NWS_USER_AGENT` - identify your app/contact per NWS's API policy (same requirement as the Tempest poller's geocoding step)

```bash
sudo chown -R owm-poller:owm-poller /opt/scripts/owm-poller
```

## 4. Test manually first

```bash
cd /opt/scripts/owm-poller
sudo -u owm-poller bash -c 'set -a; source owm-poller.env; set +a; ./venv/bin/python3 owm_poller.py'
```

You should see the startup checks pass, then a write confirmation. Ctrl+C
once confirmed.

## 5. Install as a systemd service

```bash
sudo cp owm-poller.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now owm-poller
sudo systemctl status owm-poller
journalctl -u owm-poller -f
```

## 6. Grafana

Add an InfluxDB datasource with:
- **Organization**: `home`
- **Token**: the `grafana-reader-weathermap` token from step 2
- **Default Bucket**: `weathermap`
- **Query language**: Flux

Then import `owm_dashboard.json`.

## Notes
- OWM's free tier `q` parameter format is `City,State,Country` for US
  locations - the state code is required to disambiguate cities that share a
  name across states (there are multiple "Springfield"s, etc.).
- Pressure and visibility are always returned by OWM in fixed units (hPa,
  meters) regardless of the `units` parameter - the poller converts these to
  inHg/miles itself when `OWM_UNITS=imperial`.
- A single location typo (e.g. a misspelled city) won't block the other
  locations from polling - errors are logged per-location.
- National Weather Service severe weather alerts work for any US location
  covered by these coordinates, not just personal weather stations - the
  dashboard's "Active Severe Weather Alerts" panel will be green/empty most
  of the time, which is the expected all-clear state, not a bug.
