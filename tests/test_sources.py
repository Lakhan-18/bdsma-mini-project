"""Source I/O checks. No network and no real TweetsKB data are used."""
import gzip
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from contextlib import contextmanager
from src.sources import ByteLimitExceeded, LimitedReader, fetch_source, inspect_source

TEST_ROOT = Path(__file__).resolve().parents[1] / 'outputs' / 'test_scratch'
TEST_ROOT.mkdir(parents=True, exist_ok=True)


class SourcesTests(unittest.TestCase):
    def test_byte_limit(self):
        reader = LimitedReader(io.BytesIO(b'abcdef'), 5)
        self.assertEqual(reader.read(3), b'abc')
        with self.assertRaises(ByteLimitExceeded):
            reader.read(3)

    def test_local_preview_is_bounded(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            path = Path(directory) / 'example.n3.gz'
            with gzip.open(path, 'wb') as stream:
                stream.write(b'<urn:tweet:1> <urn:tag> "example" .\n' * 5000)
            report = inspect_source({'month': 'test'}, path, preview_bytes=1024)
            self.assertEqual(report['status'], 'prefix_inspected_only')
            self.assertLessEqual(len(report['preview']), 40)
            self.assertLess(report['source']['compressed_bytes_read'], 2 * 1024**2)

    def test_download_checksum_and_retry(self):
        body = gzip.compress(b'example RDF data')
        source = {'md5': hashlib.md5(body).hexdigest()}

        @contextmanager
        def response(_):
            yield io.BytesIO(body), {'content_length': str(len(body))}

        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            target = Path(directory) / 'example.gz'
            with patch('src.sources.open_source', response):
                report = fetch_source(source, target, maximum_bytes=10000, reserve_bytes=0)
                self.assertEqual(report['status'], 'downloaded_and_verified')
                self.assertEqual(target.read_bytes(), body)
                self.assertEqual(fetch_source(source, target)['status'], 'already_verified')

    def test_corruption_is_not_published(self):
        @contextmanager
        def response(_):
            yield io.BytesIO(b'wrong data'), {}

        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            target = Path(directory) / 'example.gz'
            with patch('src.sources.open_source', response):
                with self.assertRaisesRegex(ValueError, 'integrity'):
                    fetch_source({'md5': '0' * 32}, target, reserve_bytes=0)
            self.assertFalse(target.exists())
            self.assertFalse(target.with_name(target.name + '.partial').exists())

    def test_oversized_download_stops_without_publishing(self):
        @contextmanager
        def response(_):
            yield io.BytesIO(b'x' * 20), {}

        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            target = Path(directory) / 'example.gz'
            with patch('src.sources.open_source', response):
                with self.assertRaises(ByteLimitExceeded):
                    fetch_source({'md5': 'unused'}, target, maximum_bytes=10, reserve_bytes=0)
            self.assertFalse(target.exists())


if __name__ == '__main__':
    unittest.main()
