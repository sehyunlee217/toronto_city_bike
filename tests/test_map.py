import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from city_bike.api import app
from city_bike.map_grid import background_grid, empty_cell, MAX_GRID_CELLS, TORONTO_BOUNDS


class MapTests(unittest.TestCase):
    def test_background_is_bounded_and_not_an_empty_station(self):
        resolution, cells = background_grid(11, TORONTO_BOUNDS)
        self.assertLess(resolution, 11)
        self.assertGreater(len(cells), 0)
        self.assertLessEqual(len(cells), MAX_GRID_CELLS)
        cell = empty_cell(next(iter(cells)))
        self.assertEqual(cell["station_count"], 0)
        self.assertEqual(cell["supply_level"], "no_stations")
        self.assertIsNone(cell["availability_ratio"])

    def test_close_view_keeps_fine_resolution(self):
        resolution, cells = background_grid(11, (-79.381, 43.649, -79.379, 43.651))
        self.assertEqual(resolution, 11)
        self.assertGreater(len(cells), 0)

    def test_outside_toronto_has_no_background(self):
        self.assertEqual(background_grid(8, (0, 0, 1, 1)), (8, set()))

    def test_zoom_preserves_totals_and_populates_background(self):
        stations = [{"station_id": str(i), "name": "Station", "lat": 43.65 + i * .002,
                     "lon": -79.38, "capacity": 20} for i in range(3)]
        statuses = [{"station_id": str(i), "num_bikes_available": 5,
                     "num_docks_available": 15} for i in range(3)]
        with patch("city_bike.api.map_snapshot", return_value=(stations, statuses, "test")):
            for resolution in (7, 8, 9, 10, 11):
                data = TestClient(app).get(f"/h3/map-data?resolution={resolution}&west=-79.39&east=-79.37&south=43.64&north=43.67").json()
                self.assertEqual(sum(f["properties"]["total_bikes_available"] for f in data["features"]), 15)
                self.assertEqual(sum(f["properties"]["station_count"] for f in data["features"]), 3)
                self.assertTrue(any(f["properties"]["supply_level"] == "no_stations" for f in data["features"]))

    def test_geojson_boundaries_use_longitude_first_and_close_ring(self):
        stations = [{"station_id": "1", "name": "Test station", "lat": 43.65,
                     "lon": -79.38, "capacity": 20}]
        statuses = [{"station_id": "1", "num_bikes_available": 5, "num_docks_available": 15}]
        with patch("city_bike.api.fetch_station_information", return_value=stations), \
             patch("city_bike.api.fetch_station_status", return_value=statuses):
            response = TestClient(app).get("/h3/map-data?resolution=8")
        self.assertEqual(response.status_code, 200)
        feature = response.json()["features"][0]
        ring = feature["geometry"]["coordinates"][0]
        self.assertEqual(ring[0], ring[-1])
        self.assertAlmostEqual(ring[0][0], -79.38, delta=0.1)
        self.assertAlmostEqual(ring[0][1], 43.65, delta=0.1)
        self.assertEqual(feature["properties"]["total_bikes_available"], 5)
        self.assertEqual(feature["properties"]["availability_ratio"], .25)


if __name__ == "__main__":
    unittest.main()

class ExactResolutionTests(unittest.TestCase):
    def test_wide_view_keeps_very_fine_station_and_background_cells(self):
        import h3
        stations = [{"station_id": "1", "name": "Station", "lat": 43.65,
                     "lon": -79.38, "capacity": 20}]
        statuses = [{"station_id": "1", "num_bikes_available": 5, "num_docks_available": 15}]
        with patch("city_bike.api.map_snapshot", return_value=(stations, statuses, "test")):
            result = TestClient(app).get('/h3/map-data?resolution=11').json()
        self.assertEqual(result['resolution'], 11)
        self.assertTrue(all(h3.get_resolution(f['properties']['h3_cell']) == 11 for f in result['features']))
        self.assertEqual(sum(f['properties']['station_count'] for f in result['features']), 1)
        background = [f for f in result['features'] if f['properties']['station_count'] == 0]
        self.assertGreater(len(background), 0)
        self.assertLessEqual(len(background), MAX_GRID_CELLS)
