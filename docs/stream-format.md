# Separated Kafka streams

## What changes

The collector now writes two plain-JSON streams, both keyed by station ID:

| Topic | Content | Publication |
| --- | --- | --- |
| `station-metadata` | Name, coordinates, capacity, H3 ID/resolution, metadata version, schema version and metadata observation/source times | Initially, on changes, and once again after a process restart |
| `station-observations-v2` | Station ID, metadata version, event ID, timestamps, bike/dock counts, disabled counts, status and operational flags | Every successful poll |

API responses and the map remain joined views fetched directly from GBFS.
The separation applies to stored Kafka/S3 data, not the public station response.
No existing Kafka messages or S3 files are rewritten or deleted by this change.

## Join the records

Join observations to metadata using **both `station_id` and `metadata_version`**.
Metadata versions are SHA-256 hashes of the selected stable fields. A capacity,
name, location or H3-resolution change produces a new version. Changing feed or
collection timestamps alone does not. This preserves the correct historical
capacity rather than joining an old observation to today's metadata.

`observed_at` records when our collector saw that metadata, not an asserted source
effective date. Changes between polls cannot be reconstructed. Changes from A to B
and back to A reuse A's version; the metadata observation times record the changes
we saw. Metadata for disappeared stations is retained for historical joins; absence
from a later feed is not treated as an explicit deletion.

Unknown metadata is represented by `metadata_version: null`; such observations
remain in history and must use a left join rather than being silently dropped.
Missing status fields remain null, not zero. Operational flags retain the source
representation (numeric or boolean). Full raw feed objects are no longer stored;
future analyses are limited to the selected fields.

Ratio and supply level are derived, not stored: join capacity, calculate bikes /
capacity (null for missing/zero capacity), and assign the category. For current H3
availability, first select the latest observation per station, then group station
counts and capacities. Never sum every historical snapshot as current inventory.
Coordinates in metadata allow other H3 resolutions to be calculated later.

## Delivery and duplicates

Metadata is acknowledged by Kafka before related observations are published.
This is not a cross-topic transaction or a guarantee that downstream consumers
read topics in the same order. Consumers must retain/buffer unmatched references
or rejoin them when metadata arrives. S3 can receive the two streams at different
times; query complete partitions and check for unmatched versions.

Only acknowledged metadata enters the publisher's in-memory version cache.
Failures retry the pending batch. Restarts resend metadata with the same version;
deduplicate metadata by `(station_id, metadata_version)` for dimension joins, or
retain `observed_at` to study change/reappearance times. Deduplicate observations
by `event_id`. Metadata duplicates must not multiply joined observation counts.

Both topics use delete-based retention; new topics default to seven days. Existing
time/size retention limits are accepted without modification. Do not
compact metadata keyed only by station ID: that would erase older versions still
referenced by history. A future separate latest-metadata topic may use compaction.

## Migrate the running pipeline

1. Keep your existing producer running until ready to switch. Keep old data in
   `station-observations` and its S3 prefix; it remains version-1 history.
2. Edit your existing `.env` (do not overwrite credentials) to add:

   ```dotenv
   KAFKA_OBSERVATIONS_TOPIC=station-observations-v2
   KAFKA_METADATA_TOPIC=station-metadata
   ```

   Remove `KAFKA_HISTORY_TOPIC` after noting the old topic name. The old variable
   no longer selects the output topic. Do not point v2 at a v1 topic. Render uses
   these same new settings; existing deployments need their environment updated.
3. Create/verify both topics:

   ```bash
   uv run --env-file .env city-bike-topic --create
   ```

   This creates missing topics with one partition and replication factor 3;
   it does not change existing topics or delete data. If the key lacks Create
   permission, create them in Confluent with cleanup.policy=delete,
   retention.ms=604800000 (seven days), retention.bytes=-1. The producer needs Write, Describe and
   DescribeConfigs for both new topics.
4. Add both topics to the existing S3 Sink's selection and grant its identity
   read access to them. Preserve JSON input and JSON output. Keep the old topic
   selected until its backlog is archived. Verify that topic names produce
   separate S3 prefixes. A separate connector is not required for this split.
5. Configure scheduled file rotation (for example 10 minutes, using the supported
   `rotate.schedule.interval.ms` setting) so rare metadata changes reach S3 even
   without filling a 1,000-record file. Hourly folders alone do not guarantee a
   sparse metadata record is uploaded promptly. Validate connector settings in
   Confluent before applying; scheduled rotation can change delivery guarantees,
   so retain the deduplication keys described above.
6. Stop the old collector and start the updated one:

   ```bash
   uv run --env-file .env city-bike
   ```

7. Check `/ingestion/status`, both topics, and then both S3 prefixes. The first
   poll publishes metadata and observations. Following unchanged polls publish
   observations only. Check a sample observation's metadata_version exists in the
   metadata archive before declaring migration complete.

Never remove old v1 history to save space until you have chosen its retention or
conversion policy. The collector checks cleanup.policy=delete but accepts finite
or unlimited retention. It does not verify that S3 has archived every record.
Keep the connector's lag shorter than the retention window. A size limit can expire
records earlier than the time limit.

Metadata only publishes on changes or process restart, so a fresh consumer cannot
assume all referenced versions still exist in Kafka after retention expires. To
bootstrap historical joins, load metadata versions from S3; restarting the producer
republishes current metadata only. Flink bootstrap/state management is future work.
Retaining metadata longer than observations is also supported and usually cheap.
Sparse metadata still requires scheduled connector rotation to reach S3 promptly.

## Preview without publishing

```bash
uv run python scripts/preview_stream.py
```

Fetches the live feeds, prints one metadata/observation pair and compact payload
sizes. This does not contact Kafka, create topics or change S3.
