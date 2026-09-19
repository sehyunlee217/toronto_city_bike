"""Optional Kafka topic setup. The everyday collection code does not create topics."""

import argparse
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from city_bike.settings import Settings

HISTORY_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",
    "retention.bytes": "-1",
}


def verify_history_topic(admin: AdminClient, topic: str) -> None:
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    config = admin.describe_configs([resource], request_timeout=15)[resource].result()
    policy = config.get("cleanup.policy")
    policies = {value.strip() for value in policy.value.split(",")} if policy else set()
    if policies != {"delete"}:
        raise ValueError(
            f"Topic {topic!r} must use cleanup.policy=delete (no compaction) for the S3 history stream. "
            "Compaction can remove records before they are archived. "
            "Finite retention.ms and retention.bytes are supported; unlimited retention is not required."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create or verify the S3-bound stream topics; new topics retain seven days"
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="Create if missing; never alter an existing topic",
    )
    args = parser.parse_args()
    settings = Settings.from_env()
    admin = AdminClient(settings.kafka_config())
    for name in (settings.metadata_topic, settings.topic):
        if args.create:
            topic = NewTopic(
                name, num_partitions=1, replication_factor=3, config=HISTORY_CONFIG
            )
            try:
                admin.create_topics([topic], request_timeout=15)[name].result()
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
        verify_history_topic(admin, name)
        print(
            f"Verified {name}: cleanup.policy=delete; configured time/size retention accepted (S3 archival is not checked)"
        )
