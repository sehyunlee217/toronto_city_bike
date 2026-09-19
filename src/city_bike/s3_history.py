"""Read the S3 archive into a bounded, disposable disk index for playback.

S3 is the source of truth. No Kafka consumer or full-history Python buffer.
"""

from contextlib import closing
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from itertools import islice
from datetime import datetime
import gzip
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Lock
from time import monotonic, time

import boto3
from botocore.config import Config
from city_bike.history import HistoryStore, iso, STALE_AFTER_SECONDS

DOWNLOAD_WORKERS = 4
MAX_FILE_CACHE_BYTES = 128 * 1024 * 1024
MAX_FILE_BOUNDS = 10_000
RANGE_CACHE_SECONDS = 120
MAX_TIMELINE_FRAMES = 10_000


def observation_time(record: dict) -> float:
    return datetime.fromisoformat(
        record["collected_at"].replace("Z", "+00:00")
    ).timestamp()


class S3History:
    MAX_RANGE = 24 * 3600

    def __init__(self, bucket, observations_prefix, metadata_prefix, client=None):
        self.bucket = bucket
        self.observations_prefix = observations_prefix.rstrip("/") + "/"
        self.metadata_prefix = metadata_prefix.rstrip("/") + "/"
        self.client = client
        self.lock = Lock()
        self.temp = TemporaryDirectory(prefix="city-bike-playback-")
        self.path = str(Path(self.temp.name) / "range.sqlite")
        self.window = None
        self.loaded_at = 0
        self.generation = 0
        self.available = (None, None)
        self.object_count = 0
        self.metadata_count = 0
        self.files = OrderedDict()
        self.file_bytes = 0
        self.bounds = OrderedDict()
        self.downloads = 0
        self._initialize()

    def _connect(self):
        db = sqlite3.connect(self.path)
        db.execute("PRAGMA cache_size=-4096")
        db.execute("PRAGMA max_page_count=65536")  # 256 MiB at 4096 bytes/page
        return db

    def _initialize(self):
        with closing(self._connect()) as db:
            db.executescript("""
                CREATE TABLE observations(event TEXT PRIMARY KEY, station TEXT, t REAL, record TEXT);
                CREATE INDEX station_time ON observations(station,t DESC);
                CREATE INDEX observation_time ON observations(t);
                CREATE TABLE metadata(station TEXT, version TEXT, record TEXT, PRIMARY KEY(station,version));
                CREATE TABLE stations(station TEXT PRIMARY KEY);
            """)
            db.commit()

    def close(self):
        with self.lock:
            self.temp.cleanup()

    def _objects(self, prefix):
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                if obj["Size"] and not obj["Key"].endswith("/"):
                    if not obj["Key"].endswith((".json", ".json.gz")):
                        raise ValueError(
                            "Playback supports JSON or gzip JSON S3 archives only."
                        )
                    yield obj

    def _identity(self, obj):
        return (obj["Key"], obj.get("ETag"), str(obj.get("LastModified")), obj["Size"])

    def _download(self, obj):
        identity = self._identity(obj)
        path = Path(self.temp.name) / (
            sha256(repr(identity).encode()).hexdigest() + ".object"
        )
        body = self.client.get_object(Bucket=self.bucket, Key=obj["Key"])["Body"]
        try:
            with path.open("wb") as output:
                while chunk := body.read(65536):
                    output.write(chunk)
        except Exception:
            path.unlink(missing_ok=True)
            raise
        finally:
            body.close()
        return path

    def _needed_objects(self, batch, window):
        """Skip files whose recorded timestamps are outside the selected range."""
        needed = []
        for obj in batch:
            bounds = self.bounds.get(self._identity(obj))
            outside_window = (
                window and bounds and (bounds[1] < window[0] or bounds[0] > window[1])
            )
            if outside_window:
                self._archive_bounds(bounds)
            else:
                needed.append(obj)
        return needed

    def _read_file(self, obj, path, track_times):
        """Yield one JSON record at a time so large files stay off the Python heap."""
        earliest = latest = None
        opener = gzip.open if obj["Key"].endswith(".gz") else open
        with opener(path, "rb") as lines:
            for line in lines:
                if not line.strip():
                    continue
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("Expected one JSON object per archive line.")
                if track_times:
                    timestamp = observation_time(record)
                    earliest = (
                        timestamp if earliest is None else min(earliest, timestamp)
                    )
                    latest = timestamp if latest is None else max(latest, timestamp)
                yield record

        # Remember bounds only after the entire file has been read successfully.
        if earliest is not None:
            identity = self._identity(obj)
            self.bounds[identity] = earliest, latest
            self.bounds.move_to_end(identity)
            if len(self.bounds) > MAX_FILE_BOUNDS:
                self.bounds.popitem(last=False)
            self._archive_bounds((earliest, latest))

    def _cache_file(self, identity, path, protected):
        """Evict the least recently used files, without removing this batch's files."""
        if identity in self.files:
            return
        size = path.stat().st_size
        if size > MAX_FILE_CACHE_BYTES:
            return
        while self.files and self.file_bytes + size > MAX_FILE_CACHE_BYTES:
            candidate = next((key for key in self.files if key not in protected), None)
            if candidate is None:
                return
            evicted = self.files.pop(candidate)
            self.file_bytes -= evicted.stat().st_size
            evicted.unlink()
        self.files[identity] = path
        self.file_bytes += size

    def _remove_uncached_downloads(self, pending):
        """Also clean up downloads that finished after another file failed."""
        for identity, future in pending.items():
            try:
                path = future.result()
                if identity not in self.files:
                    path.unlink(missing_ok=True)
            except Exception:
                # The main read path reports failures. Cleanup must not hide them.
                pass

    def _read_batch(self, objects, pool, track_times):
        protected = {self._identity(obj) for obj in objects}
        pending = {}
        try:
            for obj in objects:
                identity = self._identity(obj)
                if identity not in self.files:
                    pending[identity] = pool.submit(self._download, obj)
                    self.downloads += 1

            for obj in objects:
                identity = self._identity(obj)
                if identity in self.files:
                    path = self.files[identity]
                    self.files.move_to_end(identity)
                else:
                    path = pending[identity].result()
                self.object_count += 1
                try:
                    yield from self._read_file(obj, path, track_times)
                    self._cache_file(identity, path, protected)
                finally:
                    if identity not in self.files:
                        path.unlink(missing_ok=True)
        finally:
            self._remove_uncached_downloads(pending)

    def _records(self, prefix, window=None):
        """List files, download four at a time, then stream their records."""
        if self.client is None:
            self.client = boto3.client(
                "s3",
                config=Config(
                    connect_timeout=5,
                    read_timeout=20,
                    max_pool_connections=DOWNLOAD_WORKERS,
                    retries={"max_attempts": 2, "mode": "standard"},
                ),
            )
        objects = iter(self._objects(prefix))
        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
            while batch := list(islice(objects, DOWNLOAD_WORKERS)):
                needed = self._needed_objects(batch, window)
                yield from self._read_batch(
                    needed, pool, track_times=window is not None
                )

    def _archive_bounds(self, bounds):
        low, high = self.available
        self.available = (
            bounds[0] if low is None else min(low, bounds[0]),
            bounds[1] if high is None else max(high, bounds[1]),
        )

    def _load(self, start, end):
        # Called under a lock. Only one selected range is retained per process.
        if (
            self.window == (start, end)
            and monotonic() - self.loaded_at < RANGE_CACHE_SECONDS
        ):
            return
        self.window = None
        self.object_count = 0
        self.available = (None, None)
        self.downloads = 0
        with closing(self._connect()) as db:
            db.executescript(
                "DELETE FROM observations; DELETE FROM metadata; DELETE FROM stations;"
            )
            db.commit()
            try:
                for record in self._records(self.metadata_prefix):
                    station, version = (
                        str(record["station_id"]),
                        record["metadata_version"],
                    )
                    db.execute(
                        "INSERT OR REPLACE INTO metadata VALUES(?,?,?)",
                        (station, version, json.dumps(record)),
                    )
                self.metadata_count = db.execute(
                    "SELECT count(*) FROM metadata"
                ).fetchone()[0]
                for record in self._records(
                    self.observations_prefix, (start - STALE_AFTER_SECONDS, end)
                ):
                    t = observation_time(record)
                    # Three-minute lookback seeds the first frame without future leakage.
                    if start - STALE_AFTER_SECONDS <= t <= end:
                        station = str(record["station_id"])
                        db.execute(
                            "INSERT OR IGNORE INTO observations VALUES(?,?,?,?)",
                            (record["event_id"], station, t, json.dumps(record)),
                        )
                        db.execute(
                            "INSERT OR IGNORE INTO stations VALUES(?)", (station,)
                        )
                db.commit()
            except Exception:
                db.rollback()
                raise
        self.window = start, end
        self.loaded_at = monotonic()
        self.generation += 1

    def timeline(self, start=None, end=None):
        end = time() if end is None else end
        start = end - 3600 if start is None else start
        if start >= end or end - start > self.MAX_RANGE:
            raise ValueError(
                "Choose a time range greater than zero and no longer than 24 hours."
            )
        with self.lock:
            self._load(start, end)
            with closing(self._connect()) as db:
                times = [
                    row[0]
                    for row in db.execute(
                        "SELECT DISTINCT t FROM observations WHERE t>=? AND t<=? ORDER BY t LIMIT 10001",
                        (start, end),
                    )
                ]
            if len(times) > MAX_TIMELINE_FRAMES:
                raise ValueError("Too many recording times. Select a shorter range.")
            return {
                "status": "ready",
                "source": "s3",
                "generation": self.generation,
                "frames": [{"at": iso(t), "timestamp": t} for t in times],
                "error": None,
                "metadata_records": self.metadata_count,
                "warning": None
                if self.metadata_count
                else "No station metadata was found in S3. Add station-metadata to the S3 sink connector.",
                "available_start": iso(self.available[0])
                if self.available[0] is not None
                else None,
                "available_end": iso(self.available[1])
                if self.available[1] is not None
                else None,
                "start": start,
                "end": end,
                "objects_read": self.object_count,
                "s3_downloads": self.downloads,
            }

    def frames(self, resolution, at, generation):
        with self.lock:
            if self.window is None or generation != self.generation:
                raise LookupError(
                    "The selected range is no longer cached. Load your time range again."
                )
            if not self.window[0] <= at <= self.window[1]:
                raise ValueError("Selected time is outside the loaded range.")
            store = HistoryStore()
            store.status = "ready"
            with closing(self._connect()) as db:
                # Indexed lookup returns one observation per station, not the entire history.
                rows = db.execute(
                    """SELECT o.record, m.record FROM stations s
                    JOIN observations o ON o.event=(SELECT event FROM observations
                        WHERE station=s.station AND t<=? ORDER BY t DESC,event DESC LIMIT 1)
                    LEFT JOIN metadata m ON m.station=o.station
                        AND m.version=json_extract(o.record,'$.metadata_version')""",
                    (at,),
                )
                for observation, metadata in rows:
                    if metadata:
                        store.ingest("metadata", json.loads(metadata), "metadata")
                    store.ingest("observations", json.loads(observation), "metadata")
            result = store.frames(resolution, at=at)
            result["source"] = "s3"
            return result
