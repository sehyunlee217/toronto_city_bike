"""Bounded Toronto background grid; occupied cells always retain their data."""

import math
import h3

# A study rectangle, not an administrative or land boundary.
TORONTO_BOUNDS = (-79.70, 43.55, -79.05, 43.90)
MAX_GRID_CELLS = 2500


def background_grid(resolution: int, bounds: tuple) -> tuple[int, set[str]]:
    west, south, east, north = bounds
    west, south = max(west, TORONTO_BOUNDS[0]), max(south, TORONTO_BOUNDS[1])
    east, north = min(east, TORONTO_BOUNDS[2]), min(north, TORONTO_BOUNDS[3])
    if west >= east or south >= north:
        return resolution, set()
    area = (
        6371**2
        * math.radians(east - west)
        * (math.sin(math.radians(north)) - math.sin(math.radians(south)))
    )
    # Estimate before allocating a fine grid over a wide viewport.
    while (
        resolution > 5
        and area / h3.average_hexagon_area(resolution, "km^2") > MAX_GRID_CELLS * 0.7
    ):
        resolution -= 1
    polygon = h3.LatLngPoly(
        [(south, west), (south, east), (north, east), (north, west)]
    )
    while True:
        cells = set(h3.polygon_to_cells(polygon, resolution))
        if len(cells) <= MAX_GRID_CELLS or resolution == 5:
            return resolution, cells
        resolution -= 1


def empty_cell(cell_id: str) -> dict:
    return {
        "h3_cell": cell_id,
        "station_count": 0,
        "empty_station_count": 0,
        "low_station_count": 0,
        "full_station_count": 0,
        "total_capacity": 0,
        "total_bikes_available": 0,
        "total_docks_available": 0,
        "availability_ratio": None,
        "supply_level": "no_stations",
    }


def station_background(resolution: int, bounds: tuple, occupied: set[str]) -> set[str]:
    """Keep station resolution exact; use a bounded nearby grid at wide zooms."""
    grid_resolution, cells = background_grid(resolution, bounds)
    if grid_resolution == resolution:
        return cells - occupied
    west, south, east, north = bounds
    west, south = max(west, TORONTO_BOUNDS[0]), max(south, TORONTO_BOUNDS[1])
    east, north = min(east, TORONTO_BOUNDS[2]), min(north, TORONTO_BOUNDS[3])
    candidates = set()
    for cell in sorted(occupied):
        for neighbor in sorted(h3.grid_disk(cell, 1)):
            if neighbor in occupied:
                continue
            lat, lon = h3.cell_to_latlng(neighbor)
            if west <= lon <= east and south <= lat <= north:
                candidates.add(neighbor)
                if len(candidates) >= MAX_GRID_CELLS:
                    return candidates
    return candidates
