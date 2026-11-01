"""Generated fixtures based on the inspected layout, not actual dataset results."""
import gzip
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import patch
import pyarrow.parquet as pq
from src.ingest import extract, extract_with_cache, iter_tweet_blocks, parse_tweet
from src.sources import ByteLimitExceeded

ROOT = Path(__file__).resolve().parents[1]
TEST_ROOT = ROOT / 'outputs' / 'test_scratch'
TEST_ROOT.mkdir(parents=True, exist_ok=True)
PREFIX = '''@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix sioc: <http://rdfs.org/sioc/ns#> .
@prefix sioc_t: <http://rdfs.org/sioc/types#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix schema: <http://schema.org/> .
'''
BLOCK = '''_:t123 rdf:type sioc:Post ; dc:created "2013-02-01T01:00:00"^^xsd:dateTime ; sioc:id "123" .
_:t123 schema:mentions _:tag1, _:tag2, _:entity .
_:tag1 rdf:type sioc_t:Tag ; rdfs:label "#AI" .
_:tag2 rdf:type sioc_t:Tag ; rdfs:label "ai" .
_:entity rdf:type schema:Thing ; rdfs:label "not_a_hashtag" .
'''
SECOND = '''_:t124 rdf:type sioc:Post ; dc:created "2013-02-01T02:00:00+01:00"^^xsd:dateTime ; sioc:id "124" .
_:t124 schema:mentions _:tag3 .
_:tag3 rdf:type sioc_t:Tag ; rdfs:label "Cricket" .
'''


class IngestTests(unittest.TestCase):
    def test_graph_semantics_normalization_and_timezone(self):
        tweet_id, stamp, tags, assumed = parse_tweet(PREFIX, BLOCK)
        self.assertEqual(tweet_id, '123')
        self.assertEqual(tags, ['ai'])
        self.assertTrue(assumed)
        self.assertEqual(stamp.isoformat(), '2013-02-01T01:00:00+00:00')
        _, stamp, _, assumed = parse_tweet(PREFIX, SECOND)
        self.assertFalse(assumed)
        self.assertEqual(stamp.hour, 1)

    def test_split_multiple_blocks(self):
        blocks = list(iter_tweet_blocks(io.BytesIO((PREFIX + BLOCK + '\n' + SECOND).encode())))
        self.assertEqual(len(blocks), 2)
        self.assertEqual(parse_tweet(*blocks[1])[0], '124')

    def test_missing_field_and_unresolved_mention_fail(self):
        with self.assertRaises(ValueError):
            parse_tweet(PREFIX, BLOCK.replace('sioc:id "123"', 'sioc:name "123"'))
        with self.assertRaisesRegex(ValueError, 'not defined'):
            parse_tweet(PREFIX, BLOCK.replace('_:tag1 rdf:type sioc_t:Tag ; rdfs:label "#AI" .', ''))

    def test_decoded_and_block_limits(self):
        with self.assertRaises(ByteLimitExceeded):
            list(iter_tweet_blocks(io.BytesIO((PREFIX + BLOCK).encode()), max_decoded_bytes=10))
        with self.assertRaises(ByteLimitExceeded):
            list(iter_tweet_blocks(io.BytesIO((PREFIX + BLOCK).encode()), max_block_bytes=150))

    def test_extract_parquet_and_integrity_failure_cleanup(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            folder = Path(directory)
            archive = folder / 'test.gz'
            body = gzip.compress((PREFIX + BLOCK + '\n' + SECOND).encode())
            archive.write_bytes(body)
            source = {'month': '2013-02', 'md5': hashlib.md5(body).hexdigest()}
            target = folder / 'events.parquet'
            report = extract(source, target, archive, batch_rows=1, reserve_bytes=0)
            rows = pq.read_table(target).to_pylist()
            self.assertEqual([row['hashtag'] for row in rows], ['ai', 'cricket'])
            self.assertEqual(report['tweets_scanned'], 2)
            self.assertEqual(report['hashtag_events'], 2)
            self.assertTrue(report['publisher_md5_verified'])
            broken = folder / 'broken.parquet'
            with self.assertRaisesRegex(ValueError, 'integrity'):
                extract({**source, 'md5': '0' * 32}, broken, archive, reserve_bytes=0)
            self.assertFalse(broken.exists())
            self.assertFalse(broken.with_name(broken.name + '.partial').exists())

    def test_unsupported_layout_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            list(iter_tweet_blocks(io.BytesIO(b'<urn:x> <urn:y> <urn:z> .\n')))

    def test_truncated_gzip_is_not_published(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            folder = Path(directory)
            body = gzip.compress((PREFIX + BLOCK + SECOND).encode())[:-8]
            archive = folder / 'truncated.gz'
            archive.write_bytes(body)
            target = folder / 'events.parquet'
            with self.assertRaises(EOFError):
                extract({'month': '2013-02', 'md5': hashlib.md5(body).hexdigest()}, target, archive, reserve_bytes=0)
            self.assertFalse(target.exists())
            self.assertFalse(target.with_name(target.name + '.partial').exists())

    def test_owned_temporary_archive_removed_after_success(self):
        body = gzip.compress((PREFIX + BLOCK + SECOND).encode())
        source = {'month': '2013-02', 'md5': hashlib.md5(body).hexdigest()}

        @contextmanager
        def response(_):
            yield io.BytesIO(body), {'content_length': str(len(body))}

        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            folder = Path(directory)
            cache = folder / 'cache.gz'
            with patch('src.sources.open_source', response):
                report = extract_with_cache(source, folder / 'events.parquet', cache, reserve_bytes=0)
            self.assertTrue(report['temporary_archive_removed'])
            self.assertFalse(cache.exists())
            self.assertTrue((folder / 'events.parquet').exists())


if __name__ == '__main__':
    unittest.main()
