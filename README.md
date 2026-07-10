# Weather Monitoring Stack

Two independent data pipelines feeding a shared InfluxDB instance and Grafana:

- **Tempest** — polls your own WeatherFlow Tempest station(s) directly
- **OpenWeatherMap** — polls current conditions for arbitrary cities

Both pipelines also independently check the National Weather Service for
active severe weather alerts at their respective locations.

## Architecture

![Deployment architecture](deployment-architecture.svg)

- **Poller host** runs both poller services and InfluxDB 2.x. Each
  poller writes to its own bucket (`tempest`, `weathermap`) so the two
  pipelines stay fully independent even though they share a host. This can
  be a single VM/container, or you can split InfluxDB onto its own host if
  you prefer.
- **Grafana host** runs Grafana, which connects to InfluxDB over the
  network (port 8086) using two separate least-privilege read tokens — one
  per bucket. This can be the same host as the pollers, or a separate one.
- NWS alerts are polled by **both** pollers independently, each on its own
  5-minute cycle, rather than as a shared/deduplicated service.

## Setup guides

- [`SETUP-TEMPEST.md`](SETUP-TEMPEST.md) — Tempest poller, InfluxDB, and the Tempest Grafana dashboard
- [`SETUP-OWM.md`](SETUP-OWM.md) — OpenWeatherMap poller and its Grafana dashboard

## Files

| File | Purpose |
|---|---|
| `tempest_poller.py` | Polls WeatherFlow Tempest station(s) → InfluxDB |
| `tempest-poller.env.example` | Config template for the Tempest poller |
| `tempest-poller.service` | systemd unit for the Tempest poller |
| `tempest_dashboard.json` | Grafana dashboard for Tempest data |
| `owm_poller.py` | Polls OpenWeatherMap for configured cities → InfluxDB |
| `owm-poller.env.example` | Config template for the OpenWeatherMap poller |
| `owm-poller.service` | systemd unit for the OpenWeatherMap poller |
| `owm_dashboard.json` | Grafana dashboard for OpenWeatherMap data |
| `requirements.txt` | Shared Python dependencies for both pollers |
| `deployment-architecture.svg` | Architecture diagram (this README) |
