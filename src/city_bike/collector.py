"""Single background collector. Failed batches retain their event IDs on retry."""

from datetime import datetime, timezone
import logging
from threading import Event, Lock
from time import monotonic

from city_bike.observations import collect_observations
from city_bike.settings import Settings

logger = logging.getLogger(__name__)


class Collector:
    def __init__(self, settings: Settings, publisher):
        self.settings = settings
        self.publisher = publisher
        self.stop = Event()
        self.lock = Lock()
        self.last_success_at = None
        self.last_success_clock = None
        self.last_batch_size = 0
        self.failure_count = 0
        self.pending_batch = None

    def snapshot(self) -> dict:
        with self.lock:
            fresh = self.last_success_clock is not None and (
                monotonic() - self.last_success_clock
                < max(180, self.settings.poll_seconds * 3)
            )
            return {
                "enabled": True,
                "status": "collecting"
                if fresh and not self.failure_count
                else "degraded",
                "last_success_at": self.last_success_at,
                "last_batch_size": self.last_batch_size,
                "consecutive_failures": self.failure_count,
            }

    def collect_once(self) -> None:
        """Fetch one batch, send it to Kafka, and record a successful delivery."""
        if self.pending_batch is None:
            self.pending_batch = collect_observations(self.settings.h3_resolution)

        # If this raises, keep the batch for retry with exactly the same event IDs.
        self.publisher.publish(self.pending_batch)
        self._record_success(len(self.pending_batch["observations"]))
        self.pending_batch = None

    def run(self) -> None:
        """Repeat collection until the web server asks this background thread to stop."""
        try:
            while not self.stop.is_set():
                try:
                    self.collect_once()
                except Exception as error:
                    self._record_failure(error)
                # Like sleep(), but shutdown can interrupt the wait immediately.
                self.stop.wait(self.settings.poll_seconds)
        finally:
            self.publisher.close()

    def _record_success(self, observation_count: int) -> None:
        with self.lock:
            self.last_success_at = datetime.now(timezone.utc).isoformat()
            self.last_success_clock = monotonic()
            self.last_batch_size = observation_count
            self.failure_count = 0
        logger.info("Confirmed %d observations in Kafka", observation_count)

    def _record_failure(self, error: Exception) -> None:
        # Log the error type only: exception messages may contain connection secrets.
        logger.error(
            "Collection failed (%s); retrying next interval", type(error).__name__
        )
        with self.lock:
            self.failure_count += 1
