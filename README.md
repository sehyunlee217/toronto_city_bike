# Reading the code

New to Kafka? Start with [the three-file guide](docs/start-here.md). Then follow [the full walkthrough](docs/code-walkthrough.md) from the source API through Kafka, S3, and map playback.

# City Bike

A learning project for building a small real-time data pipeline with Bike Share
Toronto GBFS data.

## Step 1: Load GBFS Data

GBFS stands for General Bikeshare Feed Specification. Bike-share systems expose
JSON feeds that describe station metadata and live station status.

This project currently reads two Bike Share Toronto feeds:

- `station_information`: mostly stable station metadata such as name, location,
  address, and capacity.
- `station_status`: frequently changing station state such as available bikes,
  available docks, and whether the station is renting or returning.

Run the preview:

```bash
uv run python scripts/preview_gbfs.py
```

Expected result:

- A count of station information records.
- A sample station information record.
- A count of station status records.
- A sample station status record.

The important idea is that `station_id` connects the two feeds. For example,
station `7000` in `station_information` is the same physical station as station
`7000` in `station_status`.

## Collect historical observations

The service runs a background poller that publishes compact station observations
and versioned metadata to separate Kafka topics. See [Stream format and migration](docs/stream-format.md)
for field meanings, historical joins, S3 changes and switching an existing collector.

```text
GBFS → Python collector → station-metadata (initially and on changes)
                       → station-observations-v2 (each poll)
Both topics → S3 connector → separate historical file prefixes
```

Observations contain counts, status, timestamps and a metadata reference. Metadata
contains name, location, capacity and H3 information. Full raw source objects and
repeated static fields are no longer sent with every observation. Missing metadata
is represented by a null reference. Ratios are calculated from counts and capacity.

The home page `/` displays a live H3 map with selectable cells, automatic zoom-based
resolution (5–11), bike/dock totals and a 60-second refresh. Manual sizes 7–11 are
also available. Gray translucent cells show areas without reported stations in a
Toronto study rectangle (including water), not stations with zero bikes. Background
cells are generated for the visible area, with a density cap that coarsens very
large requests. The current effective resolution is displayed below the status.
Network totals stay consistent across zoom levels; station coordinates are
re-indexed at each resolution, rather than treating hexagon edges as exact nested
boundaries. Map feed snapshots are cached for 30 seconds to avoid repeated upstream
requests during zooming and panning. Run `uv run city-bike` and open
`http://localhost:8000/`. Restart an already-running server to pick up new routes.
No Kafka credentials are required to view the map. Leaflet loads from a CDN and
OpenStreetMap provides the basemap, so an internet connection is needed.

The `/stations/*`, `/h3/availability` and `/h3/map-data` endpoints fetch GBFS directly.
The map uses `/h3/map-data` for GeoJSON hexagon boundaries and availability; the
existing `/h3/availability` route remains JSON. Totals include all reported station
states. Flink processing and a Kafka-backed map cache are future steps. S3 archiving is configured separately in Confluent.

### 1. Configure Confluent

Create a Confluent Cloud Kafka cluster in a region that also supports Flink for the
next stage. Create Kafka cluster API credentials. The setup identity needs topic
creation and configuration-read permissions. The running service needs Write,
Describe and DescribeConfigs on both stream topics (and any cluster permission
required for idempotent publishing by your cluster).

Copy `.env.example` to `.env` and fill in the bootstrap servers, API key and secret.
Never commit `.env`. The service only reads environment variables; the commands
below ask uv to load the file explicitly.

```bash
uv sync --frozen
uv run --env-file .env city-bike-topic --create
uv run --env-file .env city-bike-topic
```

The create command creates each missing stream topic with one partition, replication factor 3 and:

```properties
cleanup.policy=delete
retention.ms=604800000
retention.bytes=-1
```

New topics retain seven days of Kafka data, with no size-based expiration by
default. Existing topics are never altered. Startup and verification accept your
configured time and size limits, including unlimited retention; they only enforce
cleanup.policy=delete to prevent compaction from removing unarchived observations
or metadata versions. S3 is the long-term archive. Keep retention longer than the
connector's maximum expected outage/backlog, and verify both streams reach S3.
Finite retention can expire records even if the connector has not read them.
Verification checks Kafka configuration, not S3 archive completeness.

Values are plain UTF-8 JSON, not Schema Registry-framed messages. Explicitly
configure the input format/schema when adding Flink or an archive connector.

### 2. Run locally

```bash
uv run --env-file .env city-bike
```

Open `http://localhost:8000/docs` for the API and
`http://localhost:8000/ingestion/status` for collection status. `last_success_at`
advances only when every record in a batch has been acknowledged by Kafka.
`/health` is process liveness, not evidence that collection is succeeding; monitor
the ingestion status and alert on `degraded` or a stale success timestamp.

Without `COLLECTOR_ENABLED=true`, the API runs without Kafka credentials:

```bash
uv run city-bike
```

### 3. Deploy on Render

Push the repository (including `uv.lock`, `Dockerfile` and `render.yaml`) to GitHub.
In Render, create a Blueprint from that repository. Review the paid web-service
plan and enter the three Kafka connection variables when prompted. Create and
verify both topics before deploying. Render builds the image and runs one
instance; no separate container registry or persistent disk is required.

The container listens on Render's `PORT` and runs as a non-root user. The Blueprint
uses `/health` and allows 120 seconds for shutdown. Use one instance and one
Uvicorn worker; additional workers/instances each start a separate poller.
Rolling deployments can briefly overlap collectors and produce extra observations.
Check `/ingestion/status` at your Render URL and inspect records in Confluent's
topic viewer to confirm end-to-end delivery.

For a local container build:

```bash
docker build -t city-bike .
docker run --rm --env-file .env -p 8000:8000 city-bike
```

### Delivery and gaps

The producer uses TLS, idempotence and acknowledgements from all in-sync replicas.
Failed batches are retried with the same event IDs while the process stays alive.
Application retries can still create duplicate records; deduplicate by `event_id`
when analyzing history. Separate polls deliberately have separate event IDs,
even if the source station values have not changed.

Unconfirmed batches are held in memory, not on disk. A process crash during an
outage can lose that pending batch. While retrying a failed batch, the collector
does not fetch new snapshots. Poller downtime therefore creates collection gaps;
Kafka retention cannot recover GBFS observations that were never collected.
On a normal shutdown the current operation finishes and the producer is flushed.

### Longer-term archive

Before accumulating months of data, add a managed Confluent S3 Sink connector
from both `station-metadata` and `station-observations-v2` to a bucket you control,
organized by topic and date. Enable scheduled rotation for sparse metadata (see the
migration guide). Start with JSON for these schemaless topics, or introduce a supported schema before Parquet.
Verify exported counts, dates and sample records before reducing Kafka retention
to 7–30 days. Keep bucket expiration disabled or explicitly set to your desired
history period. Connector compute, Kafka storage and S3 are separately billable.

### Checks

```bash
uv run python -m unittest discover -s tests -v
```

Tests run without cloud credentials and cover record preservation, stable retry
IDs, retention guards, delivery failures, collector retries and API startup.

References: [Confluent topic settings](https://docs.confluent.io/cloud/current/topics/manage.html),
[S3 connector](https://docs.confluent.io/cloud/current/connectors/cc-s3-sink/cc-s3-sink.html),
[Render Blueprints](https://render.com/docs/blueprint-spec).

## Read and try the API documentation

Open `/docs`, expand an endpoint, click **Try it out**, and then **Execute**.
The actual JSON appears under **Server response → Response body**. Opening an
endpoint alone does not call it. **Parameters** are inputs; **Responses → Schema**
and the **Schemas** section describe returned fields. Examples are illustrative.

| Endpoint | Inputs | Output |
| --- | --- | --- |
| `/stations/current` | None | One object per matched station; H3 resolution 8 |
| `/stations/empty` | None | In-service stations reporting zero bikes |
| `/stations/low-availability` | `max_ratio`, default `0.2` | In-service stations at or below the ratio, including empty stations with known capacity |
| `/h3/availability` | `resolution`, default `8` | One object per occupied H3 cell with combined station totals |
| `/h3/map-data` | `resolution` and viewport bounds | GeoJSON cell shapes, totals, background grid and fetch time |
| `/ingestion/status` | None | Whether the optional Kafka history collector is disabled, collecting or degraded |
| `/health` | None | API liveness only |

`is_renting` and `is_returning` describe pickup/return permissions, independently
of available bike/dock counts. `last_reported` is the source station timestamp in
Unix seconds. A cell's availability ratio is total bikes divided by total capacity.
`{"enabled":false,"status":"disabled"}` is expected when history collection is off;
it does not disable the live map. JSON viewer labels such as `[100…199]` group array
positions and are not API fields. Restart an existing server to load updated docs.

## S3 archive playback

Open `/history`, choose From/To dates in your local timezone (up to 24 hours), and
click **Load archive**. Drag the slider or press Play to move between archived
collection times. H3 resolution automatically follows map zoom (5–11). Zooming does not download S3 files again.
The archive can lag behind live collection because the sink uploads in batches.
Reload a range to check for newly archived readings (same-range cache TTL: two minutes).

Set `HISTORY_ENABLED=true`, `S3_HISTORY_BUCKET`, `S3_OBSERVATIONS_PREFIX`, and
`S3_METADATA_PREFIX`; see `.env.example`. AWS credentials use boto3's standard
credential chain: environment, AWS profile, or an attached role. Keys are server-side
only. Grant `s3:ListBucket` for the bucket and `s3:GetObject` for both topic prefixes;
SSE-KMS objects also require decrypt permissions. Both topics must be archived.
Current support is one JSON record per line, optionally gzip-compressed.

Playback no longer starts a Kafka consumer or keeps the complete history in RAM.
It streams S3 files into a disposable SQLite index containing only the selected
range plus a three-minute lookback and the versioned metadata. The index is capped
at 256 MiB with a small SQLite memory cache; the journal can require additional
temporary disk space. Only one station snapshot is assembled in Python for each
frame. S3 remains the source of truth; the temporary index is removed on normal
shutdown. No external database, Spark, Athena, or Flink is needed for playback.

The first load lists both archive prefixes and reads files using four parallel GETs.
A 128 MiB LRU disk cache reuses files across selected ranges; object identity includes
ETag, size, and modification time. A bounded manifest of actual timestamp ranges
lets subsequent loads skip irrelevant files. It filters by actual `collected_at`
rather than assuming an S3 folder timestamp matches collection time. This preserves delayed/retried records.
It does not reread the bucket on slider movement. The file cache and manifest are temporary and rebuilt after restart. For a large
archive, persist the manifest or generate partitioned playback summaries offline.
In-flight downloads require temporary disk space in addition to the cache/index. Loading long ranges may be slow or exceed the disk cap;
choose a smaller range if that happens. No silent truncation is performed.

`GET /history/timeline?start=...&end=...` accepts Unix seconds and returns recording
times, archive bounds, and a `generation`. `GET /history/data?at=...&generation=...&resolution=8`
returns one frame. One range is cached per server process; another visitor loading
a different range invalidates earlier generations, returning HTTP 409 with a reload
message. Use one worker for this implementation. Multi-user deployment should add
session-specific bounded caches before serving heavy public traffic.

Frames use the latest observation at or before the selected timestamp and join
metadata by exact station ID/version. Missing metadata is counted and excluded;
stale or incomplete cells appear gray. A station with no record in the loaded range
or its three-minute lookback is absent. Repeated events are deduplicated by event ID.
Playback does not infer rentals from availability changes or interpolate readings.

For deployment, configure the AWS and S3 variables in the hosting provider's secret
settings. `.env` is not included in the Docker image. Kafka collection remains
independent and still uses the existing Kafka credentials and retention settings.

Both maps scale H3 cells automatically with zoom. Station cells always use the
requested resolution; the gray live-map background is limited to nearby cells
when a complete fine grid would be too large. No manual size selector is needed.
