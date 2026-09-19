from concurrent.futures import Future
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from city_bike.api import app
from city_bike.collector import Collector
from city_bike.kafka import KafkaPublisher
from city_bike.topics import HISTORY_CONFIG, verify_history_topic
from city_bike.observations import build_batch
from city_bike.settings import Settings


INFO = {"last_updated": 100, "data": {"stations": [
    {"station_id": "1", "lat": 43.65, "lon": -79.38, "capacity": 20},
]}}
STATUS = {"last_updated": 200, "data": {"stations": [
    {"station_id": "1", "num_bikes_available": 4, "last_reported": 190},
]}}


class ObservationTests(unittest.TestCase):
    def test_compact_records_reference_stable_metadata(self):
        batch = build_batch(INFO, STATUS, poll_id="poll-1")
        row, metadata = batch["observations"][0], batch["metadata"][0]
        self.assertEqual(row["metadata_version"], metadata["metadata_version"])
        self.assertEqual(row["schema_version"], 2)
        self.assertNotIn("capacity", row)
        self.assertNotIn("raw_station_status", row)
        self.assertNotIn("h3_cell", row)
        self.assertEqual(metadata["capacity"], 20)
        second = build_batch(INFO | {"last_updated": 999}, STATUS, poll_id="poll-2")
        self.assertEqual(metadata["metadata_version"], second["metadata"][0]["metadata_version"])
        self.assertNotEqual(row["event_id"], second["observations"][0]["event_id"])
        self.assertEqual(row["event_id"], build_batch(INFO, STATUS, poll_id="poll-1")["observations"][0]["event_id"])

    def test_capacity_changes_create_new_metadata_version(self):
        changed = {"data": {"stations": [INFO["data"]["stations"][0] | {"capacity": 30}]}}
        self.assertNotEqual(build_batch(INFO, STATUS)["metadata"][0]["metadata_version"],
                            build_batch(changed, STATUS)["metadata"][0]["metadata_version"])

    def test_unknown_station_is_preserved_without_inventing_metadata(self):
        row = build_batch({"data": {"stations": []}}, STATUS)["observations"][0]
        self.assertIsNone(row["metadata_version"])
        self.assertEqual(row["station_id"], "1")

    def test_empty_feed_fails(self):
        with self.assertRaises(ValueError):
            build_batch(INFO, {"data": {"stations": []}})


class PublishingTests(unittest.TestCase):
    def publisher(self, error=None, remaining=0):
        publisher = KafkaPublisher.__new__(KafkaPublisher)
        publisher.topic = "history"
        publisher.metadata_topic = "metadata"
        publisher.metadata_versions = {}
        publisher.producer = Mock()
        publisher.producer.flush.return_value = remaining
        publisher.producer.produce.side_effect = lambda *a, **kw: kw["on_delivery"](error, None)
        return publisher

    def test_serializes_and_waits_for_delivery(self):
        publisher = self.publisher()
        publisher.publish(build_batch(INFO, STATUS))
        kwargs = publisher.producer.produce.call_args.kwargs
        self.assertEqual(kwargs["key"], b"1")
        self.assertEqual(json.loads(kwargs["value"])["num_bikes_available"], 4)
        self.assertEqual(publisher.producer.flush.call_count, 2)

    def test_delivery_failure_or_timeout_is_not_success(self):
        for publisher in [self.publisher(error="failed"), self.publisher(remaining=1)]:
            with self.assertRaises(RuntimeError):
                publisher.publish(build_batch(INFO, STATUS))

    def test_unchanged_metadata_is_published_once_and_changes_are_sent_first(self):
        publisher = self.publisher()
        publisher.publish(build_batch(INFO, STATUS))
        publisher.publish(build_batch(INFO, STATUS))
        changed = {"data": {"stations": [INFO["data"]["stations"][0] | {"capacity": 30}]}}
        publisher.publish(build_batch(changed, STATUS))
        self.assertEqual([call.args[0] for call in publisher.producer.produce.call_args_list],
                         ["metadata", "history", "history", "metadata", "history"])

    def test_unconfirmed_metadata_does_not_advance_cache_or_publish_observation(self):
        publisher = self.publisher(error="failed")
        with self.assertRaises(RuntimeError):
            publisher.publish(build_batch(INFO, STATUS))
        self.assertEqual(publisher.metadata_versions, {})
        self.assertEqual([call.args[0] for call in publisher.producer.produce.call_args_list], ["metadata"])

    def test_observation_retry_reuses_acknowledged_metadata(self):
        publisher = self.publisher()
        publisher.producer.flush.side_effect = [0, 1, 0]
        batch = build_batch(INFO, STATUS)
        with self.assertRaises(RuntimeError):
            publisher.publish(batch)
        publisher.publish(batch)
        self.assertEqual([call.args[0] for call in publisher.producer.produce.call_args_list],
                         ["metadata", "history", "history"])

    def test_restart_republishes_metadata_with_same_version(self):
        first, second = self.publisher(), self.publisher()
        first.publish(build_batch(INFO, STATUS))
        second.publish(build_batch(INFO, STATUS))
        a = json.loads(first.producer.produce.call_args_list[0].kwargs["value"])
        b = json.loads(second.producer.produce.call_args_list[0].kwargs["value"])
        self.assertEqual(a["metadata_version"], b["metadata_version"])

    def test_topic_retention_guard(self):
        for overrides, safe in [({}, True), ({"cleanup.policy": "compact"}, False),
                                ({"cleanup.policy": "compact,delete"}, False),
                                ({"cleanup.policy": " delete "}, True),
                                ({"retention.ms": "604800000"}, True),
                                ({"retention.ms": "-1"}, True),
                                ({"retention.bytes": "1000"}, True)]:
            admin = Mock()
            config = {k: SimpleNamespace(value=v) for k, v in (HISTORY_CONFIG | overrides).items()}
            def describe(resources, **kwargs):
                future = Future()
                future.set_result(config)
                return {resources[0]: future}
            admin.describe_configs.side_effect = describe
            if safe:
                verify_history_topic(admin, "history")
            else:
                with self.assertRaises(ValueError):
                    verify_history_topic(admin, "history")


class CollectorTests(unittest.TestCase):
    def test_failed_batch_is_retried_without_refetch_or_new_event_ids(self):
        publisher = Mock()
        collector = Collector(Settings(), publisher)
        collector.stop = Mock()
        collector.stop.is_set.side_effect = [False, False, True]
        publisher.publish.side_effect = [RuntimeError("failed"), None]
        batch = build_batch(INFO, STATUS)
        with patch("city_bike.collector.collect_observations", return_value=batch) as fetch:
            collector.run()
        fetch.assert_called_once()
        self.assertIs(publisher.publish.call_args_list[0].args[0], publisher.publish.call_args_list[1].args[0])
        self.assertEqual(collector.snapshot()["status"], "collecting")
        self.assertEqual(collector.snapshot()["last_batch_size"], 1)
        publisher.close.assert_called_once()

    def test_failure_does_not_advance_success(self):
        publisher = Mock()
        publisher.publish.side_effect = RuntimeError("failed")
        collector = Collector(Settings(), publisher)
        collector.stop = Mock()
        collector.stop.is_set.side_effect = [False, True]
        with patch("city_bike.collector.collect_observations", return_value=build_batch(INFO, STATUS)):
            collector.run()
        self.assertIsNone(collector.snapshot()["last_success_at"])
        self.assertEqual(collector.snapshot()["consecutive_failures"], 1)


class ServiceTests(unittest.TestCase):
    def test_api_runs_without_cloud_credentials(self):
        with patch.dict(os.environ, {"COLLECTOR_ENABLED": "false"}, clear=True):
            with TestClient(app) as client:
                self.assertEqual(client.get("/health").status_code, 200)
                self.assertEqual(client.get("/ingestion/status").json()["status"], "disabled")

    def test_enabled_collector_requires_credentials(self):
        with patch.dict(os.environ, {"COLLECTOR_ENABLED": "true"}, clear=True):
            with self.assertRaises(ValueError):
                with TestClient(app):
                    pass

    def test_invalid_poll_intervals(self):
        for value in ["0", "-1", "nan", "inf"]:
            with patch.dict(os.environ, {"POLL_INTERVAL_SECONDS": value}, clear=True):
                with self.assertRaises(ValueError):
                    Settings.from_env()

    def test_legacy_env_does_not_mix_v2_into_old_topic(self):
        with patch.dict(os.environ, {"KAFKA_HISTORY_TOPIC": "station-observations"}, clear=True):
            self.assertEqual(Settings.from_env().topic, "station-observations-v2")
        with patch.dict(os.environ, {"KAFKA_OBSERVATIONS_TOPIC": "station-observations"}, clear=True):
            with self.assertRaises(ValueError):
                Settings.from_env()


if __name__ == "__main__":
    unittest.main()
