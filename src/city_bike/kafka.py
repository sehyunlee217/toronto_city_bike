"""Sending records to Confluent Cloud. This module does not run Flink or write S3."""

import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from city_bike.settings import Settings
from city_bike.topics import verify_history_topic


class KafkaPublisher:
    """Send one poll's records to Kafka, waiting until delivery is confirmed."""

    def __init__(self, settings: Settings):
        config = settings.kafka_config()
        admin = AdminClient(config)
        for topic in (settings.topic, settings.metadata_topic):
            verify_history_topic(admin, topic)
        self.topic = settings.topic
        self.metadata_topic = settings.metadata_topic
        self.metadata_versions = {}
        self.producer = Producer(
            {
                **config,
                "enable.idempotence": True,  # Protect against duplicates from internal producer retries.
                "acks": "all",  # Require acknowledgment from all in-sync replicas.
                "compression.type": "gzip",  # Compress messages during transport/storage.
                "delivery.timeout.ms": 30000,  # Fail a delivery after 30 seconds.
            }
        )

    def publish(self, batch: dict) -> None:
        """Send station details first, then availability readings that refer to them."""
        changed = []
        for station in batch["metadata"]:
            last_sent_version = self.metadata_versions.get(station["station_id"])
            if last_sent_version != station["metadata_version"]:
                changed.append(station)
        if changed:
            self._publish_records(self.metadata_topic, changed)
            # Only acknowledged metadata is eligible to be referenced by observations.
            for record in changed:
                self.metadata_versions[record["station_id"]] = record[
                    "metadata_version"
                ]
        self._publish_records(self.topic, batch["observations"])

    def _publish_records(self, topic: str, records: list[dict]) -> None:
        errors = []

        def delivered(error, message):
            if error is not None:
                errors.append(error)

        try:
            for record in records:
                # A topic is a named stream. The key groups readings by station;
                # the value is the JSON record, encoded as bytes for Kafka.
                self.producer.produce(
                    topic,
                    key=record["station_id"].encode(),
                    value=json.dumps(
                        record, separators=(",", ":"), allow_nan=False
                    ).encode(),
                    on_delivery=delivered,
                )
                self.producer.poll(0)  # Process delivery notifications without waiting.
        finally:
            remaining = self.producer.flush(35)  # Wait up to 35 seconds for delivery.
        if remaining or errors:
            raise RuntimeError(
                f"Kafka batch not confirmed: {remaining} pending, {len(errors)} failed"
            )

    def close(self) -> None:
        self.producer.flush(35)
