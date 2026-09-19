import unittest
from city_bike.history import HistoryStore, iso


class PlaybackTests(unittest.TestCase):
    def setUp(self):
        self.store = HistoryStore()
        self.meta = dict(station_id='1', metadata_version='a', lat=43.65, lon=-79.38, capacity=10)
        self.store.ingest('metadata', self.meta, 'metadata', now=1000)

    def observation(self, timestamp, bikes, version='a', event=None):
        self.store.ingest('observations', dict(station_id='1', metadata_version=version,
            event_id=event or str(timestamp), collected_at=iso(timestamp),
            num_bikes_available=bikes, num_docks_available=10-bikes), 'metadata', now=1000)

    def test_reconstructs_without_future_values_and_deduplicates(self):
        self.observation(900, 0)
        self.observation(690, 5)
        self.observation(900, 0)
        result = self.store.frames(8, now=1000)
        self.assertEqual(len(result['frames']), 31)
        self.assertEqual(len(self.store.observations['1']), 2)
        self.assertEqual(result['frames'][0]['cells'][0]['availability_ratio'], .5)
        self.assertEqual(result['frames'][20]['cells'][0]['supply_level'], 'empty')
        self.assertEqual(result['frames'][19]['cells'][0]['supply_level'], 'unknown')
        polygon = next(iter(result['geometries'].values()))['coordinates'][0]
        self.assertEqual(polygon[0], polygon[-1])

    def test_exact_metadata_version_not_latest_and_missing_metadata(self):
        self.store.ingest('metadata', {**self.meta, 'metadata_version':'b', 'capacity':20}, 'metadata', now=1000)
        self.observation(990, 5)
        self.assertEqual(self.store.frames(8, now=1000)['frames'][-1]['cells'][0]['availability_ratio'], .5)
        self.observation(1000, 5, 'missing')
        frame = self.store.frames(8, now=1000)['frames'][-1]
        self.assertEqual(frame['missing_metadata'], 1)
        self.assertEqual(frame['cells'], [])

    def test_stale_values_are_unknown_and_old_history_remains_available(self):
        self.observation(700, 5)
        frame = self.store.frames(8, now=1000)['frames'][-1]
        self.assertEqual(frame['stale_stations'], 1)
        self.assertIsNone(frame['cells'][0]['total_bikes_available'])
        self.assertIsNone(frame['cells'][0]['availability_ratio'])
        self.assertEqual(self.store.frames(8, now=1400, at=700)['frames'][0]['cells'][0]['total_bikes_available'], 5)
        self.assertEqual(self.store.timeline()['frames'], [{'at': iso(700), 'timestamp': 700}])

    def test_full_timeline_sorted_unique_and_selected_time_has_no_future_leak(self):
        self.observation(900, 0)
        self.observation(100, 5)
        self.observation(900, 0)
        self.assertEqual(self.store.timeline()['frames'], [{'at': iso(100), 'timestamp': 100}, {'at': iso(900), 'timestamp': 900}])
        self.assertEqual(self.store.frames(8, now=1000, at=100)['frames'][0]['cells'][0]['total_bikes_available'], 5)
        self.assertEqual(self.store.frames(8, now=1000, at=99)['frames'][0]['cells'], [])

    def test_timeline_preserves_submillisecond_precision(self):
        self.observation(900.123456, 5)
        moment = self.store.timeline()['frames'][0]
        frame = self.store.frames(8, now=1000, at=moment['timestamp'])['frames'][0]
        self.assertEqual(frame['station_count'], 1)
