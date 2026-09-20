# Toronto Bike Share

Bike availability across 1,070 Toronto stations, with a live H3 map and historical playback.

[Live demo](https://toronto-city-bike.onrender.com/) · [History](https://toronto-city-bike.onrender.com/history)

A Python collector polls the Bike Share Toronto GBFS feeds every minute and sends
observations to Kafka on Confluent Cloud. An S3 sink archives the records for playback.
Station metadata is stored separately from availability readings.

FastAPI serves both maps. The live map reads the GBFS feeds directly; playback reads
S3 with a temporary disk cache. Hexagons adjust automatically as you zoom. Hosted on Render.

## Run locally

With [uv](https://docs.astral.sh/uv/) installed:

```bash
uv sync
uv run city-bike
```

Open http://localhost:8000. The live map works without cloud credentials. Collection
and playback require the settings in `.env.example`; see the [setup notes](docs/setup.md).

## Notes

- [Start here](docs/start-here.md) — collection and Kafka basics
- [Code walkthrough](docs/code-walkthrough.md) — how the pieces connect
- [Stream format](docs/stream-format.md) — records and metadata versions

Collecting historical data for future demand analysis. ML and Flink are not implemented yet.
