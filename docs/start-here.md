# Start here

You do not need to understand Flink to run this project. No Flink job is configured.
For now, read just three files in this order.

## 1. Fetch and prepare data: observations.py

`collect_observations()` fetches two public Bike Share feeds:

- Station information: name, location, and capacity.
- Station status: available bikes, available docks, and operational flags.

It passes them to `build_batch()`. The returned dictionary has two lists:

```python
{
    "metadata": [...],      # Station details, with a version identifying their contents.
    "observations": [...],  # Readings that refer to the matching station details.
}
```

An observation is one station at one collection time. A batch is all stations from
one poll. These are ordinary Python dictionaries and lists before Kafka sees them.

## 2. Repeat the work: collector.py

The core operation is `Collector.collect_once()`:

```python
if self.pending_batch is None:
    self.pending_batch = collect_observations(self.settings.h3_resolution)

self.publisher.publish(self.pending_batch)
self._record_success(len(self.pending_batch["observations"]))
self.pending_batch = None
```

Read it as: get readings, send them, record success, clear the finished batch.
If sending raises an error, Python skips the remaining lines. The batch stays in
`pending_batch` so the next attempt sends the same readings with the same IDs.

`run()` repeatedly calls this operation and waits for the configured interval.
The other methods update `/ingestion/status`; you can read them later.

## 3. Send the batch: kafka.py

`KafkaPublisher.publish()` sends changed station details first, followed by the
availability observations. The publisher remembers which metadata versions Kafka
has acknowledged, so unchanged details do not need to be sent every minute.

The few Kafka terms you need:

| Term | Meaning in this project |
| --- | --- |
| Topic | A named stream: `station-metadata` or `station-observations-v2`. |
| Record/message | One station metadata object or one availability observation. |
| Producer | Our Python code that sends records. |
| Consumer | Something that reads records; the managed S3 sink is one example. |
| Key | The station ID sent alongside a record, used to choose its partition. |
| Partition | One ordered portion of a topic. Ordering is within a partition. |
| Retention | How long Kafka keeps records, configured in Confluent Cloud. |

`produce()` queues a message for sending. `poll(0)` processes delivery notifications.
`flush(35)` waits up to 35 seconds for outstanding messages. Only confirmed delivery
counts as success; the collector retries an unconfirmed batch. Stable event IDs
allow the archive reader to remove duplicates from those application-level retries.

You can leave the producer's connection, compression, and acknowledgment settings
alone while learning the data flow. Their comments explain why they are there.
`topics.py` contains the optional setup command; it does not need to be understood
to follow each collected reading. Existing topic retention is not changed by startup.

## Run the existing app

With your configured `.env`:

```bash
uv run --env-file .env city-bike
```

Open `http://localhost:8000/` for the live map, `/history` for S3 playback, and
`/ingestion/status` to check the collector. Avoid running a second collector while
one is already active; two collectors would both publish readings.

To run only the website against your existing S3 archive:

```bash
COLLECTOR_ENABLED=false uv run --env-file .env city-bike
```

Keep `HISTORY_ENABLED=true` in `.env` for playback. To explore just the live map
without cloud configuration:

```bash
COLLECTOR_ENABLED=false HISTORY_ENABLED=false uv run city-bike
```

The live map fetches the public feeds directly. The Confluent S3 sink archives Kafka
records independently. Playback reads those S3 files; the website does not need
Flink for either map. This means a working live map alone does not verify Kafka.

## What to change while learning

- Polling interval: `POLL_INTERVAL_SECONDS` in `.env` (restart after changes).
- Fields in an observation: `build_observations()` in `observations.py`.
- Availability color thresholds: `classify_supply_level()` in `availability.py`.

Changing stored record fields may require coordinated schema/version and reader
changes. First trace the existing fields before changing the archive format.

Once these three files make sense, use [the full walkthrough](code-walkthrough.md)
for FastAPI, H3, S3 playback, caching, and the current limitations.
