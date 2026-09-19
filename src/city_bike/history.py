"""Reconstruct an H3 frame from a small set of selected observations."""

from bisect import bisect_right
from datetime import datetime, timezone
from threading import Lock
from time import time

import h3

from city_bike.availability import (
    build_h3_availability,
    classify_supply_level,
    calculate_availability_ratio,
)


STALE_AFTER_SECONDS = 180


def iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


class HistoryStore:
    def __init__(self):
        self.lock = Lock()
        self.metadata = {}
        self.observations = {}
        self.status = "loading"
        self.error = None
        self.skipped_records = 0
        self.event_ids = set()
        self.timestamps = set()

    def ingest(self, topic, record, metadata_topic, now=None):
        now = time() if now is None else now
        station = str(record["station_id"])
        with self.lock:
            if topic == metadata_topic:
                self.metadata[(station, record["metadata_version"])] = record
                return
            timestamp = datetime.fromisoformat(
                record["collected_at"].replace("Z", "+00:00")
            ).timestamp()
            if timestamp > now + 60:
                return
            rows = self.observations.setdefault(station, [])
            if record["event_id"] in self.event_ids:
                return
            self.event_ids.add(record["event_id"])
            self.timestamps.add(timestamp)
            rows.append((timestamp, record))
            if len(rows) > 1 and rows[-2][0] > timestamp:
                rows.sort(key=lambda item: (item[0], item[1]["event_id"]))

    def timeline(self):
        with self.lock:
            return {
                "status": self.status,
                "error": self.error,
                "frames": [
                    {"at": iso(t), "timestamp": t} for t in sorted(self.timestamps)
                ],
                "skipped_records": self.skipped_records,
            }

    def _frame(self, rows_by_station, metadata, resolution, frame_time):
        """Choose one reading per station, join its metadata, and group by H3."""
        stations = []
        missing_metadata = 0
        stale_stations = 0
        for station_id, rows in rows_by_station.items():
            reading = latest_reading(rows, frame_time)
            if reading is None:
                continue
            timestamp, record = reading
            station_metadata = metadata.get(
                (station_id, record.get("metadata_version"))
            )
            if (
                station_metadata is None
                or station_metadata.get("lat") is None
                or station_metadata.get("lon") is None
            ):
                missing_metadata += 1
                continue
            is_stale = frame_time - timestamp > STALE_AFTER_SECONDS
            if is_stale:
                stale_stations += 1
            stations.append(
                station_for_map(record, station_metadata, resolution, is_stale)
            )

        return {
            "at": iso(frame_time),
            "cells": historical_cells(stations),
            "station_count": len(stations),
            "missing_metadata": missing_metadata,
            "stale_stations": stale_stations,
        }

    def frames(self, resolution, now=None, at=None):
        now = time() if now is None else now
        end = int(now // 10) * 10
        frame_times = [at] if at is not None else range(end - 300, end + 1, 10)
        with self.lock:
            rows_by_station = {
                station: list(rows) for station, rows in self.observations.items()
            }
            metadata = dict(self.metadata)
            status, error, skipped = self.status, self.error, self.skipped_records

        frames = []
        geometries = {}
        for frame_time in frame_times:
            frame = self._frame(rows_by_station, metadata, resolution, frame_time)
            frames.append(frame)
            for cell in frame["cells"]:
                cell_id = cell["h3_cell"]
                if cell_id not in geometries:
                    geometries[cell_id] = cell_geometry(cell_id)

        timestamps = [
            timestamp for rows in rows_by_station.values() for timestamp, _ in rows
        ]
        latest = max(timestamps, default=None)
        return {
            "status": status,
            "error": error,
            "resolution": resolution,
            "time_basis": "collected_at",
            "latest_observation_at": iso(latest) if latest else None,
            "skipped_records": skipped,
            "geometries": geometries,
            "frames": frames,
        }


def latest_reading(rows, at):
    """Find the last reading at or before the selected time (never a future one)."""
    timestamps = [timestamp for timestamp, _ in rows]
    index = bisect_right(timestamps, at) - 1
    return rows[index] if index >= 0 else None


def station_for_map(record, metadata, resolution, is_stale):
    capacity = metadata.get("capacity")
    bikes = None if is_stale else record.get("num_bikes_available")
    ratio = calculate_availability_ratio(bikes, capacity)
    return {
        "h3_cell": h3.latlng_to_cell(metadata["lat"], metadata["lon"], resolution),
        "capacity": capacity,
        "num_bikes_available": bikes,
        "num_docks_available": None if is_stale else record.get("num_docks_available"),
        "supply_level": classify_supply_level(ratio),
        "unknown": ratio is None,
    }


def historical_cells(stations):
    cells = build_h3_availability(stations)
    unknown_cells = {station["h3_cell"] for station in stations if station["unknown"]}
    for cell in cells:
        # Partial station data must not look like a complete regional total.
        if cell["h3_cell"] in unknown_cells:
            cell["availability_ratio"] = None
            cell["supply_level"] = "unknown"
            cell["total_bikes_available"] = None
            cell["total_docks_available"] = None
    return cells


def cell_geometry(cell_id):
    # H3 returns latitude/longitude; GeoJSON expects longitude/latitude.
    ring = [[lon, lat] for lat, lon in h3.cell_to_boundary(cell_id)]
    return {"type": "Polygon", "coordinates": [ring + [ring[0]]]}
