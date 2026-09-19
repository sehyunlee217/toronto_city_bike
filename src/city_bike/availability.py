import h3


def build_station_availability(
    stations: list[dict],
    statuses: list[dict],
    h3_resolution: int = 8,
) -> list[dict]:
    statuses_by_station_id = {status["station_id"]: status for status in statuses}

    availability = []

    for station in stations:
        status = statuses_by_station_id.get(station["station_id"])
        if status is None:
            continue

        capacity = station.get("capacity")
        bikes_available = status.get("num_bikes_available")
        availability_ratio = calculate_availability_ratio(bikes_available, capacity)

        availability.append(
            {
                "station_id": station["station_id"],
                "name": station["name"],
                "lat": station["lat"],
                "lon": station["lon"],
                "h3_cell": h3.latlng_to_cell(
                    station["lat"],
                    station["lon"],
                    h3_resolution,
                ),
                "capacity": capacity,
                "num_bikes_available": bikes_available,
                "num_docks_available": status.get("num_docks_available"),
                "status": status.get("status"),
                "is_renting": gbfs_flag_to_bool(status.get("is_renting")),
                "is_returning": gbfs_flag_to_bool(status.get("is_returning")),
                "last_reported": status.get("last_reported"),
                "availability_ratio": availability_ratio,
                "supply_level": classify_supply_level(availability_ratio),
            }
        )

    return availability


def calculate_availability_ratio(
    bikes_available: int | None,
    capacity: int | None,
) -> float | None:
    if bikes_available is None or capacity in (None, 0):
        return None

    return bikes_available / capacity


def classify_supply_level(availability_ratio: float | None) -> str:
    """
    Currently set
    0% -> Empty
    0%-20% -> Low
    20%-90% -> Healthy
    90%-100% -> Full
    """

    if availability_ratio is None:
        return "unknown"
    if availability_ratio == 0:
        return "empty"
    if availability_ratio <= 0.2:
        return "low"
    if availability_ratio >= 0.9:
        return "full"
    return "healthy"


def gbfs_flag_to_bool(value: int | bool | None) -> bool | None:
    if value is None:
        return None

    return bool(value)


def build_h3_availability(stations: list[dict]) -> list[dict]:
    cells: dict[str, dict] = {}

    for station in stations:
        h3_cell = station["h3_cell"]
        cell = cells.setdefault(
            h3_cell,
            {
                "h3_cell": h3_cell,
                "station_count": 0,
                "empty_station_count": 0,
                "low_station_count": 0,
                "full_station_count": 0,
                "total_capacity": 0,
                "total_bikes_available": 0,
                "total_docks_available": 0,
            },
        )

        cell["station_count"] += 1
        cell["total_capacity"] += station["capacity"] or 0
        cell["total_bikes_available"] += station["num_bikes_available"] or 0
        cell["total_docks_available"] += station["num_docks_available"] or 0

        if station["supply_level"] == "empty":
            cell["empty_station_count"] += 1
        elif station["supply_level"] == "low":
            cell["low_station_count"] += 1
        elif station["supply_level"] == "full":
            cell["full_station_count"] += 1

    for cell in cells.values():
        cell["availability_ratio"] = calculate_availability_ratio(
            cell["total_bikes_available"],
            cell["total_capacity"],
        )
        cell["supply_level"] = classify_supply_level(cell["availability_ratio"])

    return sorted(
        cells.values(),
        key=lambda cell: (
            cell["empty_station_count"],
            cell["low_station_count"],
            -cell["availability_ratio"]
            if cell["availability_ratio"] is not None
            else 0,
        ),
        reverse=True,
    )
