import asyncio
from contextlib import asynccontextmanager
from threading import Thread, Lock
from time import monotonic
from pathlib import Path
from datetime import datetime, timezone

import h3

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from city_bike.collector import Collector
from city_bike.s3_history import S3History
from botocore.exceptions import BotoCoreError, ClientError
import sqlite3
from city_bike.kafka import KafkaPublisher
from city_bike.settings import Settings
from city_bike.map_grid import station_background, empty_cell
from city_bike.schemas import (
    StationAvailability,
    H3Availability,
    MapData,
    HealthStatus,
    IngestionDisabled,
    IngestionActive,
)

from city_bike.gbfs_client import fetch_station_information, fetch_station_status
from city_bike.availability import (
    build_h3_availability,
    build_station_availability,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    app.state.collector = None
    thread = None
    app.state.history = None
    if (
        settings.enabled
    ):  # When we want to run our confluent - Kakfa - S3 Data Collection
        publisher = await asyncio.to_thread(KafkaPublisher, settings)
        app.state.collector = Collector(settings, publisher)
        thread = Thread(target=app.state.collector.run, name="history-collector")
        thread.start()
    if settings.history_enabled and settings.s3_bucket:
        app.state.history = S3History(
            settings.s3_bucket,
            settings.s3_observations_prefix,
            settings.s3_metadata_prefix,
        )
    try:
        yield
    finally:
        if app.state.history is not None:
            await asyncio.to_thread(app.state.history.close)
        if thread is not None:
            app.state.collector.stop.set()
            await asyncio.to_thread(thread.join)


app = FastAPI(
    lifespan=lifespan,
    title="Toronto Bike Share API",
    description=(
        "Live Toronto Bike Share data. **Parameters are inputs you send; response fields are outputs you receive.** "
        "Expand an endpoint, click **Try it out**, then **Execute** to see its actual Response body. "
        "Use **Responses → Schema** (or the Schemas section below) for field descriptions. "
        "Station routes return one item per station; H3 routes combine stations by geographic cell. "
        "Live station and map routes read GBFS directly and do not require Kafka or Flink. "
        "Historical collection is separate and disabled unless configured. Stored events use separate versioned station-metadata and station-observations-v2 topics; these API responses remain joined live views. Examples are illustrative, not live readings."
    ),
    openapi_tags=[
        {
            "name": "Stations",
            "description": "Individual stations joined by station_id. Missing status matches are omitted.",
        },
        {
            "name": "H3 cells",
            "description": "Stations grouped geographically. Includes all matched operational states, not only IN_SERVICE stations.",
        },
        {
            "name": "Service status",
            "description": "API liveness and the optional Kafka history collector.",
        },
    ],
    version="0.1.0",
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def map_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/history", include_in_schema=False)
def history_page():
    return FileResponse(STATIC_DIR / "history.html")


def archive_reader(request):
    reader = request.app.state.history
    if reader is None:
        raise HTTPException(
            503,
            "Set HISTORY_ENABLED=true, S3_HISTORY_BUCKET and AWS read credentials to enable archive playback.",
        )
    return reader


def archive_call(action):
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(409, str(exc)) from exc
    except (BotoCoreError, ClientError) as exc:
        raise HTTPException(
            503,
            "Could not read the S3 archive. Check AWS credentials, bucket region, ListBucket and GetObject access.",
        ) from exc
    except sqlite3.Error as exc:
        raise HTTPException(
            503,
            "The temporary archive index could not be written. Check disk space or select a shorter range (256 MiB index limit).",
        ) from exc
    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(
            422,
            "Invalid range or unsupported archive records. Choose up to 24 hours; archives must contain the versioned observation and metadata JSON records.",
        ) from exc


@app.get(
    "/history/data",
    tags=["H3 cells"],
    summary="Replay an S3 observation at a selected time",
    description="Load /history/timeline first and pass its generation. Returns one H3 frame; records older than three minutes are unknown. Metadata is joined by station ID and exact version.",
)
def history_data(
    request: Request,
    at: float = Query(..., ge=0, allow_inf_nan=False),
    generation: int = Query(..., ge=1),
    resolution: int = Query(8, ge=5, le=11),
) -> dict:
    reader = archive_reader(request)
    return archive_call(lambda: reader.frames(resolution, at, generation))


@app.get(
    "/history/timeline",
    tags=["H3 cells"],
    summary="Load a time range from the S3 archive",
    description="Unix seconds start/end; defaults to the past hour, maximum 24 hours. Streams archive JSON into a disposable disk index, never a full-history memory buffer. The response lists recorded times and a generation for frame requests. One range is cached per service process for two minutes; loading another range invalidates prior generations.",
)
def history_timeline(
    request: Request,
    start: float | None = Query(None, ge=0, allow_inf_nan=False),
    end: float | None = Query(None, ge=0, allow_inf_nan=False),
) -> dict:
    reader = archive_reader(request)
    return archive_call(lambda: reader.timeline(start, end))


_map_snapshot_lock = Lock()
_map_snapshot = None


def map_snapshot():
    global _map_snapshot
    with _map_snapshot_lock:
        if _map_snapshot is None or monotonic() - _map_snapshot[0] >= 30:
            stations = fetch_station_information()
            statuses = fetch_station_status()
            _map_snapshot = (
                monotonic(),
                stations,
                statuses,
                datetime.now(timezone.utc).isoformat(),
            )
        return _map_snapshot[1:]


@app.get(
    "/h3/map-data",
    tags=["H3 cells"],
    response_model=MapData,
    summary="Get hexagon shapes and availability for the map",
    description="Returns GeoJSON polygons and totals, including gray no-station cells. Bounds limit only the background grid, clipped to the Toronto study rectangle; occupied cells still cover the whole network. Station cells always use the requested resolution. Feed snapshots are cached for 30 seconds. The background limit can coarsen the actual resolution for all cells. No Kafka or Flink required.",
)
def map_data(
    resolution: int = Query(
        8,
        ge=5,
        le=11,
        description="Requested H3 detail: higher numbers mean smaller cells. The station resolution is exact; background cells are limited at wide zooms.",
    ),
    west: float = Query(
        -79.70, ge=-180, le=180, description="Western viewport longitude, in degrees."
    ),
    south: float = Query(
        43.55, ge=-85, le=85, description="Southern viewport latitude, in degrees."
    ),
    east: float = Query(
        -79.05, ge=-180, le=180, description="Eastern viewport longitude, in degrees."
    ),
    north: float = Query(
        43.90, ge=-85, le=85, description="Northern viewport latitude, in degrees."
    ),
) -> dict:
    stations, statuses, fetched_at = map_snapshot()
    availability = build_station_availability(
        stations, statuses, h3_resolution=resolution
    )
    cells = build_h3_availability(availability)
    occupied = {cell["h3_cell"] for cell in cells}
    background = station_background(resolution, (west, south, east, north), occupied)
    cells.extend(empty_cell(cell) for cell in sorted(background - occupied))
    features = []
    for cell in cells:
        ring = [[lon, lat] for lat, lon in h3.cell_to_boundary(cell["h3_cell"])]
        ring.append(ring[0])
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [ring]},
                "properties": cell,
            }
        )
    return {
        "type": "FeatureCollection",
        "features": features,
        "fetched_at": fetched_at,
        "resolution": resolution,
    }


@app.get(
    "/health",
    tags=["Service status"],
    response_model=HealthStatus,
    summary="Check whether the API is responding",
    description="Liveness check only. An ok response does not mean Kafka collection is enabled or station data is fresh.",
)
def health() -> dict:
    return {"status": "ok"}


@app.get(
    "/ingestion/status",
    tags=["Service status"],
    response_model=IngestionDisabled | IngestionActive,
    summary="Check the optional Kafka history collector",
    description="Always returns a JSON object. When COLLECTOR_ENABLED=false, returns enabled=false and status=disabled; the map and live station endpoints still work. Enabled collectors report confirmed Kafka delivery, including a degraded state before the first successful batch. This does not report Flink status.",
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "disabled": {
                            "summary": "Collector disabled (normal for local map preview)",
                            "value": {"enabled": False, "status": "disabled"},
                        },
                        "collecting": {
                            "summary": "Kafka confirmed a batch",
                            "value": {
                                "enabled": True,
                                "status": "collecting",
                                "last_success_at": "2026-09-19T12:00:00+00:00",
                                "last_batch_size": 1070,
                                "consecutive_failures": 0,
                            },
                        },
                        "starting": {
                            "summary": "Enabled, no successful batch yet",
                            "value": {
                                "enabled": True,
                                "status": "degraded",
                                "last_success_at": None,
                                "last_batch_size": 0,
                                "consecutive_failures": 0,
                            },
                        },
                    }
                }
            }
        }
    },
)
def ingestion_status(request: Request) -> dict:
    collector = request.app.state.collector
    return (
        collector.snapshot() if collector else {"enabled": False, "status": "disabled"}
    )


@app.get(
    "/stations/current",
    tags=["Stations"],
    response_model=list[StationAvailability],
    summary="Get current availability for each station",
    description="No input parameters. Returns one object per station with matching metadata and status. Fields such as station_id, capacity and is_renting are response fields, not request parameters. Includes all matched operational states; H3 IDs use resolution 8. Fetches GBFS on each request and does not save history.",
)
def current_stations() -> list[dict]:
    stations = fetch_station_information()
    statuses = fetch_station_status()
    return build_station_availability(stations, statuses)


@app.get(
    "/stations/empty",
    tags=["Stations"],
    response_model=list[StationAvailability],
    summary="Find in-service stations reporting zero bikes",
    description="Filters stations to num_bikes_available=0 and source status=IN_SERVICE. Does not additionally filter by is_renting or is_returning. An empty list means no stations matched.",
)
def empty_stations() -> list[dict]:
    stations = fetch_station_information()
    statuses = fetch_station_status()
    availability = build_station_availability(stations, statuses)

    return [
        station
        for station in availability
        if station["num_bikes_available"] == 0 and station["status"] == "IN_SERVICE"
    ]


@app.get(
    "/stations/low-availability",
    tags=["Stations"],
    response_model=list[StationAvailability],
    summary="Find in-service stations at or below a bike availability threshold",
    description="Returns IN_SERVICE stations with a known bikes/capacity ratio at or below max_ratio. Includes zero-bike stations when capacity is known and nonzero. Missing ratios are excluded; pickup/return permissions are reported separately.",
)
def low_availability_stations(
    max_ratio: float = Query(
        0.2,
        ge=0,
        le=1,
        description="Inclusive bikes/capacity threshold. For example, 0.2 means 20%; this is a fraction, not a bike count.",
    ),
) -> list[dict]:
    stations = fetch_station_information()
    statuses = fetch_station_status()
    availability = build_station_availability(stations, statuses)

    return [
        station
        for station in availability
        if station["availability_ratio"] is not None
        and station["availability_ratio"] <= max_ratio
        and station["status"] == "IN_SERVICE"
    ]


@app.get(
    "/h3/availability",
    tags=["H3 cells"],
    response_model=list[H3Availability],
    summary="Combine station availability by H3 cell",
    description="Returns one item per occupied cell, not per station. Sums station capacity, bikes and docks; cell availability is total bikes divided by total capacity, not an average of station ratios. Includes all matched operational states. No polygon shapes or gray background cells; use /h3/map-data for those. Fetches GBFS directly on each request.",
)
def h3_availability(
    resolution: int = Query(
        8,
        ge=0,
        le=15,
        description="H3 resolution from 0 to 15. Higher numbers mean smaller cells and usually fewer stations per group. Does not guarantee one station per cell.",
    ),
) -> list[dict]:
    stations = fetch_station_information()
    statuses = fetch_station_status()
    availability = build_station_availability(
        stations, statuses, h3_resolution=resolution
    )
    return build_h3_availability(availability)
