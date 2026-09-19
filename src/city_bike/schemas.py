"""Documented response contracts for the public API."""

from typing import Literal

from pydantic import BaseModel, Field

SupplyLevel = Literal["unknown", "empty", "low", "healthy", "full"]


class StationAvailability(BaseModel):
    """One station with matched metadata and current status; not a historical record."""

    station_id: str = Field(
        description="Station identifier from the source feed.", examples=["7118"]
    )
    name: str = Field(
        description="Station's display name.",
        examples=["King St W / Bay St (East Side)"],
    )
    lat: float = Field(description="Latitude in decimal degrees.", examples=[43.648575])
    lon: float = Field(
        description="Longitude in decimal degrees.", examples=[-79.380042]
    )
    h3_cell: str = Field(
        description="H3 cell containing this station, at resolution 8 for the station endpoints.",
        examples=["882b9bc461fffff"],
    )
    capacity: int | None = Field(
        description="Reported station capacity; null when unavailable.", examples=[19]
    )
    num_bikes_available: int | None = Field(
        description="Reported available bikes. Check is_renting separately for pickup permission.",
        examples=[1],
    )
    num_docks_available: int | None = Field(
        description="Reported available docks. Check is_returning separately for return permission.",
        examples=[16],
    )
    status: str | None = Field(
        description="Source operational status, such as IN_SERVICE; null if missing.",
        examples=["IN_SERVICE"],
    )
    is_renting: bool | None = Field(
        description="Whether pickups are allowed: true=yes, false=no, null=unknown. Does not guarantee a bike is available.",
        examples=[True],
    )
    is_returning: bool | None = Field(
        description="Whether returns are allowed: true=yes, false=no, null=unknown. Does not guarantee a dock is available.",
        examples=[True],
    )
    last_reported: int | None = Field(
        description="Station's source report time as Unix seconds since 1970-01-01 UTC, not the API request time.",
        examples=[1789838802],
    )
    availability_ratio: float | None = Field(
        description="Available bikes divided by capacity. Null if bikes or capacity are missing, or capacity is zero.",
        examples=[0.05263157894736842],
    )
    supply_level: SupplyLevel = Field(
        description="empty: ratio=0; low: ratio<=0.2 after the empty check; full: ratio>=0.9; healthy: otherwise; unknown: null ratio. Full is a supply category, not a guarantee that every dock is occupied.",
        examples=["low"],
    )


class H3Availability(BaseModel):
    """Combined station totals for one occupied H3 cell, not one station."""

    h3_cell: str = Field(
        description="Cell identifier at the requested resolution.",
        examples=["882b9bc461fffff"],
    )
    station_count: int = Field(
        description="Number of matched stations in this cell.", examples=[2]
    )
    empty_station_count: int = Field(
        description="Stations classified as empty (zero bikes with a known positive capacity).",
        examples=[0],
    )
    low_station_count: int = Field(
        description="Stations classified as low, excluding empty stations.",
        examples=[1],
    )
    full_station_count: int = Field(
        description="Stations with a bike-to-capacity ratio of at least 0.9.",
        examples=[0],
    )
    total_capacity: int = Field(
        description="Sum of station capacities. Missing capacities contribute zero.",
        examples=[40],
    )
    total_bikes_available: int = Field(
        description="Sum of available bikes. Missing counts contribute zero.",
        examples=[10],
    )
    total_docks_available: int = Field(
        description="Sum of available docks. Missing counts contribute zero.",
        examples=[30],
    )
    availability_ratio: float | None = Field(
        description="Total bikes divided by total capacity, not an average of station ratios. Null if total capacity is zero.",
        examples=[0.25],
    )
    supply_level: SupplyLevel = Field(
        description="Classification of the combined availability ratio using the station thresholds.",
        examples=["healthy"],
    )


class MapCell(H3Availability):
    """Occupied cell or generated background cell with no reported stations."""

    supply_level: Literal[
        "unknown", "empty", "low", "healthy", "full", "no_stations"
    ] = Field(
        description="Adds no_stations for gray background cells: station_count=0 and availability_ratio=null. This differs from empty, which means stations have zero bikes."
    )


class PolygonGeometry(BaseModel):
    type: Literal["Polygon"] = Field(description="GeoJSON geometry type.")
    coordinates: list[list[list[float]]] = Field(
        description="Polygon rings containing [longitude, latitude] pairs. The first point is repeated at the end to close each ring."
    )


class MapFeature(BaseModel):
    type: Literal["Feature"] = Field(description="GeoJSON feature type.")
    geometry: PolygonGeometry
    properties: MapCell


class MapData(BaseModel):
    type: Literal["FeatureCollection"] = Field(description="GeoJSON collection type.")
    features: list[MapFeature] = Field(
        description="All occupied network cells plus generated no-station cells for the visible study area."
    )
    fetched_at: str = Field(
        description="ISO 8601 UTC time when the app fetched the cached feeds; not an individual station's report time."
    )
    resolution: int = Field(
        description="Requested H3 resolution, used for every returned cell. Background coverage may be limited at fine resolutions."
    )


class HealthStatus(BaseModel):
    status: Literal["ok"] = Field(
        description="The API process can respond. Does not verify GBFS freshness, Kafka, or historical collection."
    )


class IngestionDisabled(BaseModel):
    """Expected response when COLLECTOR_ENABLED=false; live endpoints still work."""

    enabled: Literal[False]
    status: Literal["disabled"] = Field(
        description="The Kafka history collector is turned off. This is not a map or API failure."
    )


class IngestionActive(BaseModel):
    """Collector status, including startup, errors and confirmed Kafka delivery."""

    enabled: Literal[True]
    status: Literal["collecting", "degraded"] = Field(
        description="collecting: recent confirmed batch and no consecutive failures. degraded: no confirmed batch yet, stale success, or a failed attempt."
    )
    last_success_at: str | None = Field(
        description="ISO 8601 UTC time when all records in the last successful batch were acknowledged by Kafka; null before first success."
    )
    last_batch_size: int = Field(
        description="Number of observations in the last confirmed batch; zero before first success."
    )
    consecutive_failures: int = Field(
        description="Failed collection/publish attempts since the last successful batch."
    )
