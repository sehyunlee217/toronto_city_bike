import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from city_bike.api import app


class DocumentationTests(unittest.TestCase):
    def test_openapi_distinguishes_inputs_and_outputs(self):
        schema = app.openapi()
        current = schema['paths']['/stations/current']['get']
        self.assertEqual(current.get('parameters', []), [])
        self.assertEqual(current['responses']['200']['content']['application/json']['schema']['items']['$ref'],
                         '#/components/schemas/StationAvailability')
        fields = schema['components']['schemas']['StationAvailability']['properties']
        self.assertTrue(all(field.get('description') for field in fields.values()))
        examples = schema['paths']['/ingestion/status']['get']['responses']['200']['content']['application/json']['examples']
        self.assertEqual(examples['disabled']['value'], {'enabled': False, 'status': 'disabled'})

    def test_station_and_aggregate_responses_keep_existing_fields(self):
        station = {'station_id': '7118', 'name': 'Example', 'lat': 43.65, 'lon': -79.38, 'capacity': 19}
        status = {'station_id': '7118', 'num_bikes_available': 1, 'num_docks_available': 16,
                  'status': 'IN_SERVICE', 'is_renting': 1, 'is_returning': 0}
        with patch('city_bike.api.fetch_station_information', return_value=[station]), \
             patch('city_bike.api.fetch_station_status', return_value=[status]):
            client = TestClient(app)
            result = client.get('/stations/current')
            self.assertEqual(result.status_code, 200)
            row = result.json()[0]
            self.assertIs(row['is_renting'], True)
            self.assertIs(row['is_returning'], False)
            self.assertIsNone(row['last_reported'])
            for endpoint in ['/stations/low-availability', '/stations/empty', '/h3/availability']:
                self.assertEqual(client.get(endpoint).status_code, 200)
