"""Versioned station metadata and compact, append-only status observations."""

from datetime import datetime, timezone
from hashlib import sha256
import json
from uuid import NAMESPACE_URL, uuid4, uuid5

import h3

from city_bike.gbfs_client import (
    STATION_INFORMATION_URL,
    STATION_STATUS_URL,
    fetch_json,
)


def build_batch(
    information: dict,
    status: dict,
    resolution: int = 8,
    collected_at: str | None = None,
    poll_id: str | None = None,
) -> dict:
    """One poll produces static station metadata and changing availability records."""
    collected_at = collected_at or datetime.now(timezone.utc).isoformat()
    poll_id = poll_id or str(uuid4())
    metadata = build_metadata(information, resolution, collected_at)
    observations = build_observations(status, metadata, collected_at, poll_id)
    if not observations:
        raise ValueError("Station status feed contained no observations")
    return {"metadata": list(metadata.values()), "observations": observations}


def build_metadata(information: dict, resolution: int, collected_at: str) -> dict:
    """Version station attributes so old observations can join the correct location/capacity."""
    metadata = {}
    for station in information["data"]["stations"]:
        station_id = str(station["station_id"])
        lat, lon = station.get("lat"), station.get("lon")
        attributes = {
            "station_id": station_id,
            "name": station.get("name"),
            "lat": lat,
            "lon": lon,
            "capacity": station.get("capacity"),
            "h3_cell": h3.latlng_to_cell(lat, lon, resolution)
            if lat is not None and lon is not None
            else None,
            "h3_resolution": resolution,
        }
        # Timestamps are deliberately excluded: unchanged metadata keeps its version.
        version = sha256(
            json.dumps(
                attributes, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        ).hexdigest()
        metadata[station_id] = {
            "schema_version": 1,
            **attributes,
            "metadata_version": version,
            "observed_at": collected_at,
            "source_last_updated": information.get("last_updated"),
        }

    return metadata


def build_observations(
    status: dict, metadata: dict, collected_at: str, poll_id: str
) -> list[dict]:
    """Keep each reading compact: it references metadata instead of repeating it."""
    observations = []
    for row in status["data"]["stations"]:
        station_id = str(row["station_id"])
        observations.append(
            {
                "schema_version": 2,
                "event_id": str(
                    uuid5(NAMESPACE_URL, f"toronto:{poll_id}:{station_id}")
                ),
                "station_id": station_id,
                "metadata_version": metadata.get(station_id, {}).get(
                    "metadata_version"
                ),
                "collected_at": collected_at,
                "source_last_updated": status.get("last_updated"),
                "last_reported": row.get("last_reported"),
                "num_bikes_available": row.get("num_bikes_available"),
                "num_docks_available": row.get("num_docks_available"),
                "num_bikes_disabled": row.get("num_bikes_disabled"),
                "num_docks_disabled": row.get("num_docks_disabled"),
                "status": row.get("status"),
                "is_installed": row.get("is_installed"),
                "is_renting": row.get("is_renting"),
                "is_returning": row.get("is_returning"),
            }
        )
    return observations


def collect_observations(resolution: int) -> dict:
    information = fetch_json(STATION_INFORMATION_URL)
    status = fetch_json(STATION_STATUS_URL)
    return build_batch(information, status, resolution)
