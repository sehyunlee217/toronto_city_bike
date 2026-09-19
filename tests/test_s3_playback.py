import io
import hashlib
import json
import unittest
from city_bike.s3_history import S3History
from city_bike.history import iso


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.reads = 0
    def get_paginator(self, name):
        return self
    def paginate(self, Bucket, Prefix):
        # Separate pages exercise pagination.
        for key, value in self.objects.items():
            if key.startswith(Prefix):
                yield {'Contents': [{'Key': key, 'Size': len(value), 'ETag': hashlib.sha256(value).hexdigest()}]}
    def get_object(self, Bucket, Key):
        self.reads += 1
        class Body(io.BytesIO):
            def iter_lines(self):
                return iter(self.getvalue().splitlines())
        return {'Body': Body(self.objects[Key])}


def lines(*records):
    return b'\n'.join(json.dumps(r).encode() for r in records)


def obs(t, bikes, version='a'):
    return dict(station_id='1', metadata_version=version, event_id=str(t),
                collected_at=iso(t), num_bikes_available=bikes, num_docks_available=10-bikes)


class S3PlaybackTests(unittest.TestCase):
    def setUp(self):
        meta = dict(station_id='1', metadata_version='a', lat=43.65, lon=-79.38, capacity=10)
        self.s3 = FakeS3({'metadata/1.json': lines(meta, {**meta, 'metadata_version':'b', 'capacity':20}),
                         'observations/1.json': lines(obs(100, 5), obs(200.123456, 0)),
                         'observations/2.json': lines(obs(200.123456, 0), obs(300, 5, 'b'))})
        self.reader = S3History('bucket', 'observations', 'metadata', self.s3)
        self.addCleanup(self.reader.close)

    def test_archive_dedup_exact_metadata_precision_and_cached_drag(self):
        timeline = self.reader.timeline(100, 400)
        self.assertEqual(len(timeline['frames']), 3)
        self.assertEqual(timeline['metadata_records'], 2)
        reads = self.s3.reads
        generation = timeline['generation']
        frame = self.reader.frames(8, 200.123456, generation)['frames'][0]
        self.assertEqual(frame['cells'][0]['total_bikes_available'], 0)
        self.assertEqual(self.reader.frames(8, 100, generation)['frames'][0]['cells'][0]['availability_ratio'], .5)
        self.assertEqual(self.reader.frames(8, 300, generation)['frames'][0]['cells'][0]['availability_ratio'], .25)
        self.assertEqual(self.reader.timeline(100, 400)['generation'], generation)
        self.assertEqual(self.s3.reads, reads)

    def test_range_lookback_and_no_future_reading(self):
        generation = self.reader.timeline(150, 250)['generation']
        frame = self.reader.frames(8, 150, generation)['frames'][0]
        self.assertEqual(frame['cells'][0]['total_bikes_available'], 5)
        with self.assertRaises(ValueError): self.reader.frames(8, 300, generation)
        self.reader.timeline(250, 400)
        with self.assertRaises(LookupError): self.reader.frames(8, 150, generation)

    def test_missing_metadata_is_explicit_and_no_fallback(self):
        del self.s3.objects['metadata/1.json']
        timeline = self.reader.timeline(100, 400)
        self.assertIn('No station metadata', timeline['warning'])
        frame = self.reader.frames(8, 100, timeline['generation'])['frames'][0]
        self.assertEqual(frame['missing_metadata'], 1)
        self.assertEqual(frame['cells'], [])

    def test_failed_import_not_served_and_can_retry(self):
        self.s3.objects['observations/2.json'] = b'bad json'
        with self.assertRaises(ValueError): self.reader.timeline(100, 400)
        with self.assertRaises(LookupError): self.reader.frames(8, 100, 1)
        self.s3.objects['observations/2.json'] = lines(obs(300, 5))
        self.assertEqual(len(self.reader.timeline(100, 400)['frames']), 3)

    def test_invalid_ranges_and_empty_range(self):
        with self.assertRaises(ValueError): self.reader.timeline(1, 90000)
        with self.assertRaises(ValueError): self.reader.timeline(2, 1)
        self.assertEqual(self.reader.timeline(500, 600)['frames'], [])

    def test_api_requires_loaded_generation_and_valid_range(self):
        from fastapi.testclient import TestClient
        from city_bike.api import app
        from unittest.mock import patch
        with patch.object(app.state, 'history', self.reader, create=True):
            client = TestClient(app)
            response = client.get('/history/timeline', params={'start':100, 'end':400})
            self.assertEqual(response.status_code, 200)
            generation = response.json()['generation']
            self.assertEqual(client.get('/history/data', params={'at':100,'generation':generation}).status_code, 200)
            self.assertEqual(client.get('/history/data', params={'at':100,'generation':generation+1}).status_code, 409)
            self.assertEqual(client.get('/history/timeline', params={'start':1,'end':100000}).status_code, 422)
            self.assertEqual(client.get('/history/data', params={'at':'nan','generation':generation}).status_code, 422)

    def test_changed_range_reuses_files_and_skips_known_irrelevant_objects(self):
        self.reader.timeline(100, 400)
        reads = self.s3.reads
        result = self.reader.timeline(110, 150)
        self.assertEqual(self.s3.reads, reads)
        self.assertEqual(result['s3_downloads'], 0)
        self.assertEqual(result['available_end'], iso(300))

    def test_changed_object_identity_is_downloaded_again(self):
        self.reader.timeline(100, 400)
        reads = self.s3.reads
        self.s3.objects['observations/2.json'] = lines(obs(350, 2))
        result = self.reader.timeline(101, 400)
        self.assertEqual(self.s3.reads, reads + 1)
        self.assertEqual(result['frames'][-1]['timestamp'], 350)
