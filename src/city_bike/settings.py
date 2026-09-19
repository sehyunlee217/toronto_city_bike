"""Environment configuration shared by the service and topic setup command."""

from dataclasses import dataclass, field
import math
import os


@dataclass(frozen=True)
class Settings:
    enabled: bool = False
    history_enabled: bool = False
    s3_bucket: str = ""
    s3_observations_prefix: str = "topics/station-observations-v2/"
    s3_metadata_prefix: str = "topics/station-metadata/"
    bootstrap_servers: str = ""
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    topic: str = "station-observations-v2"
    metadata_topic: str = "station-metadata"
    poll_seconds: float = 60  # Change this to change data polling freq.
    h3_resolution: int = 8

    @classmethod
    def from_env(cls) -> "Settings":
        enabled = os.getenv("COLLECTOR_ENABLED", "false").lower()
        if enabled not in {"true", "false"}:
            raise ValueError("COLLECTOR_ENABLED must be true or false")
        history_enabled = os.getenv("HISTORY_ENABLED", enabled).lower()
        if history_enabled not in {"true", "false"}:
            raise ValueError("HISTORY_ENABLED must be true or false")
        settings = cls(
            history_enabled=history_enabled == "true",
            s3_bucket=os.getenv("S3_HISTORY_BUCKET", ""),
            s3_observations_prefix=os.getenv(
                "S3_OBSERVATIONS_PREFIX", "topics/station-observations-v2/"
            ),
            s3_metadata_prefix=os.getenv(
                "S3_METADATA_PREFIX", "topics/station-metadata/"
            ),
            enabled=enabled == "true",
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
            api_key=os.getenv("KAFKA_API_KEY", ""),
            api_secret=os.getenv("KAFKA_API_SECRET", ""),
            topic=os.getenv("KAFKA_OBSERVATIONS_TOPIC", "station-observations-v2"),
            metadata_topic=os.getenv("KAFKA_METADATA_TOPIC", "station-metadata"),
            poll_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "60")),
            h3_resolution=int(os.getenv("H3_RESOLUTION", "8")),
        )
        if not math.isfinite(settings.poll_seconds) or settings.poll_seconds < 10:
            raise ValueError("POLL_INTERVAL_SECONDS must be finite and at least 10")
        if not 0 <= settings.h3_resolution <= 15:
            raise ValueError("H3_RESOLUTION must be between 0 and 15")
        if not settings.metadata_topic or settings.topic == settings.metadata_topic:
            raise ValueError(
                "Observation and metadata topics must be nonempty and different"
            )
        legacy_topic = os.getenv("KAFKA_HISTORY_TOPIC", "station-observations")
        if settings.topic == legacy_topic:
            raise ValueError(
                "Use a new KAFKA_OBSERVATIONS_TOPIC for v2; do not mix with the legacy history topic"
            )
        return settings

    def kafka_config(self) -> dict:
        if not all((self.bootstrap_servers, self.api_key, self.api_secret, self.topic)):
            raise ValueError(
                "Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET and KAFKA_OBSERVATIONS_TOPIC"
            )
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.api_key,
            "sasl.password": self.api_secret,
            "client.id": "city-bike-history",
        }
