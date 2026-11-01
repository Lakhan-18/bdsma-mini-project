"""Extract a bounded monthly TweetsKB Turtle export into skinny Parquet.

This adapter targets the inspected layout: prefix declarations followed by
tweet blocks beginning with `_:t<ID> rdf:type sioc:Post`. Each block is parsed
as Turtle using RDFLib. The whole RDF graph is never retained in memory.
Unsupported layout, missing fields, unresolved blank-node mentions and damaged
archives fail the run rather than silently dropping data.
"""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import time
import unicodedata

from src.sources import ByteLimitExceeded, LimitedReader, MIB, ROOT, fetch_source, load_source, open_source

START = re.compile(rb'^_:t[0-9]+\s+(?:rdf:type|a)\s+sioc:Post\b')


class HashingReader(LimitedReader):
    def __init__(self, stream, maximum):
        super().__init__(stream, maximum)
        self.digest = hashlib.md5()

    def read(self, size=-1):
        chunk = super().read(size)
        self.digest.update(chunk)
        return chunk


def iter_tweet_blocks(stream, max_decoded_bytes=512 * MIB, max_block_bytes=MIB):
    prefixes, block = [], []
    prefix_size, block_size, total = 0, 0, 0
    for line in iter(lambda: stream.readline(max_block_bytes + 1), b''):
        total += len(line)
        if total > max_decoded_bytes:
            raise ByteLimitExceeded('Decompressed source exceeded the configured byte limit')
        if len(line) > max_block_bytes:
            raise ByteLimitExceeded('A source line exceeded the tweet block limit')
        stripped = line.strip()
        if START.match(stripped):
            if block:
                yield b''.join(prefixes).decode('utf-8-sig'), b''.join(block).decode('utf-8')
            block, block_size = [line], len(line)
        elif block:
            block.append(line)
            block_size += len(line)
        elif stripped.startswith((b'@prefix ', b'@base ', b'PREFIX ', b'BASE ')):
            prefixes.append(line)
            prefix_size += len(line)
            if prefix_size > 65536:
                raise ByteLimitExceeded('Prefix declarations exceed 64 KiB')
        elif stripped and not stripped.startswith(b'#'):
            raise ValueError('Unsupported RDF layout before first tweet; expected the inspected Turtle export')
        if block_size > max_block_bytes:
            raise ByteLimitExceeded('Tweet block exceeded the configured memory limit')
    if block:
        yield b''.join(prefixes).decode('utf-8-sig'), b''.join(block).decode('utf-8')
    else:
        raise ValueError('No tweet blocks found in source')


def parse_tweet(prefixes, block):
    from rdflib import BNode, Graph, Namespace
    from rdflib.namespace import DCTERMS, RDF, RDFS

    sioc = Namespace('http://rdfs.org/sioc/ns#')
    schema = Namespace('http://schema.org/')
    tag_type = Namespace('http://rdfs.org/sioc/types#').Tag
    graph = Graph(bind_namespaces='none')
    try:
        graph.parse(data=prefixes + '\n' + block, format='turtle')
        posts = list(graph.subjects(RDF.type, sioc.Post))
        if len(posts) != 1:
            raise ValueError(f'Expected exactly one tweet in a block, found {len(posts)}')
        post = posts[0]
        ids = list(graph.objects(post, sioc.id))
        dates = list(graph.objects(post, DCTERMS.created))
        if len(ids) != 1 or len(dates) != 1:
            raise ValueError('Tweet must have exactly one ID and creation timestamp')
        tweet_id = str(ids[0])
        if not tweet_id.isdigit():
            raise ValueError('Tweet ID is not numeric')
        timestamp = datetime.fromisoformat(str(dates[0]).replace('Z', '+00:00'))
        assumed_utc = timestamp.tzinfo is None
        if assumed_utc:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        hashtags = set()
        for mention in graph.objects(post, schema.mentions):
            types = set(graph.objects(mention, RDF.type))
            if isinstance(mention, BNode) and not types:
                raise ValueError('Mention is not defined in its tweet block; this source needs a different RDF adapter')
            if tag_type not in types:
                continue
            labels = list(graph.objects(mention, RDFS.label))
            if len(labels) != 1:
                raise ValueError('Hashtag must have exactly one label')
            tag = unicodedata.normalize('NFC', str(labels[0]).strip()).lstrip('#').casefold()
            if not tag or len(tag) > 1024:
                raise ValueError('Empty or oversized hashtag label')
            hashtags.add(tag)
        return tweet_id, timestamp, sorted(hashtags), assumed_utc
    finally:
        graph.close()


def extract(source, output, local_file=None, batch_rows=5000,
            max_compressed_bytes=20 * MIB, max_decoded_bytes=512 * MIB,
            max_output_bytes=64 * MIB, reserve_bytes=2 * 1024**3):
    import pyarrow as pa
    import pyarrow.parquet as pq

    if batch_rows < 1:
        raise ValueError('batch_rows must be positive')
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_name(output.name + '.partial')
    if output.exists() or partial.exists():
        raise FileExistsError('Output or partial file already exists; inspect it before starting another run')
    if shutil.disk_usage(output.parent).free < reserve_bytes + max_output_bytes:
        raise ValueError('Not enough free space for the output budget and reserve')
    report = {'dataset': 'TweetsKB', 'month': source['month'], 'source': 'tweetskb',
              'status': 'processing', 'tweets_scanned': 0, 'tweets_with_hashtags': 0,
              'hashtag_events': 0, 'timestamps_assumed_utc': 0,
              'tweets_outside_named_month': 0, 'min_timestamp': None, 'max_timestamp': None,
              'deduplication': 'within each tweet block; repeat tweet blocks require downstream Spark deduplication',
              'limits': {'compressed_bytes': max_compressed_bytes, 'decoded_bytes': max_decoded_bytes,
                         'parquet_bytes': max_output_bytes, 'tweet_block_bytes': MIB}}
    schema = pa.schema([('tweet_id', pa.string()), ('timestamp', pa.timestamp('us', tz='UTC')),
                        ('hashtag', pa.string())],
                       metadata={b'dataset': b'TweetsKB', b'month': source['month'].encode('ascii')})
    owned = False
    started = time.monotonic()
    minimum = maximum = None
    try:
        with open_source(source, local_file) as (raw, transport):
            length = transport.get('content_length')
            if length and int(length) > max_compressed_bytes:
                raise ByteLimitExceeded('Archive size exceeds the compressed download budget')
            reader = HashingReader(raw, max_compressed_bytes)
            report['transport'] = transport
            with partial.open('xb') as file:
                owned = True
                with pq.ParquetWriter(file, schema, compression='snappy') as writer:
                    buffer = []

                    def flush():
                        if not buffer:
                            return
                        if shutil.disk_usage(output.parent).free < reserve_bytes + MIB:
                            raise ValueError('Stopped to preserve free disk space')
                        writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                        buffer.clear()
                        file.flush()
                        if file.tell() > max_output_bytes:
                            raise ByteLimitExceeded('Parquet output exceeded the output budget')

                    with gzip.GzipFile(fileobj=reader) as decoded:
                        for prefixes, block in iter_tweet_blocks(decoded, max_decoded_bytes=max_decoded_bytes):
                            tweet_id, timestamp, hashtags, assumed_utc = parse_tweet(prefixes, block)
                            report['tweets_scanned'] += 1
                            report['timestamps_assumed_utc'] += int(assumed_utc)
                            report['tweets_with_hashtags'] += int(bool(hashtags))
                            report['tweets_outside_named_month'] += int(timestamp.strftime('%Y-%m') != source['month'])
                            minimum = timestamp if minimum is None else min(minimum, timestamp)
                            maximum = timestamp if maximum is None else max(maximum, timestamp)
                            for tag in hashtags:
                                buffer.append({'tweet_id': tweet_id, 'timestamp': timestamp, 'hashtag': tag})
                                report['hashtag_events'] += 1
                                if len(buffer) >= batch_rows:
                                    flush()
                            if report['tweets_scanned'] % 10000 == 0:
                                print(f"Scanned {report['tweets_scanned']:,} tweets; extracted {report['hashtag_events']:,} hashtag events", flush=True)
                    flush()
            if reader.digest.hexdigest() != source['md5']:
                raise ValueError('Full source failed its published MD5 integrity check')
            if not report['hashtag_events']:
                raise ValueError('Source produced no hashtag events; do not use it as a training dataset')
            if partial.stat().st_size > max_output_bytes:
                raise ByteLimitExceeded('Final Parquet file exceeded the output budget')
            report.update(status='complete', publisher_md5_verified=True,
                          compressed_bytes_read=reader.count, parquet_bytes=partial.stat().st_size,
                          min_timestamp=minimum.isoformat(), max_timestamp=maximum.isoformat(),
                          elapsed_seconds=round(time.monotonic() - started, 2), output=str(output.resolve()))
            partial.rename(output)
    except BaseException:
        if owned and partial.exists():
            partial.unlink()
        raise
    return report


def extract_with_cache(source, output, cache_path, **kwargs):
    """Use one bounded, integrity-verified archive; remove our cache on success.

    A pre-existing verified cache is retained because this invocation did not
    create it. A verified cache is also retained after extraction failure so a
    retry does not need another network download.
    """
    cache_path = Path(cache_path)
    created_here = not cache_path.exists()
    fetch_source(source, cache_path)
    report = extract(source, output, local_file=cache_path, **kwargs)
    if created_here:
        cache_path.unlink()
    report['temporary_archive_removed'] = created_here
    report['cache_path'] = str(cache_path.resolve())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month', default='2013-02')
    input_mode = parser.add_mutually_exclusive_group()
    input_mode.add_argument('--file', help='Optional local copy of the same published .n3.gz archive')
    input_mode.add_argument('--cache-small-source', action='store_true',
                            help='Download and verify one small source before parsing; remove a newly created cache on success')
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9]{4}-(?:0[1-9]|1[0-2])', args.month):
        parser.error('--month must have YYYY-MM format')
    output = ROOT / 'data' / 'extracted' / args.month / 'events.parquet'
    report_path = ROOT / 'outputs' / f'ingest_{args.month}.json'
    if output.exists():
        print(f'Output already exists: {output}. Check {report_path}; no data was overwritten.')
        return
    try:
        source = load_source(args.month)
        if args.cache_small_source:
            print(f'Downloading one small TweetsKB {args.month} archive with a 20 MiB cap before parsing.', flush=True)
            print('Its newly created temporary cache is removed after successful extraction.', flush=True)
            report = extract_with_cache(source, output, ROOT / 'data' / 'staging' / source['filename'])
        else:
            print(f"Reading real TweetsKB {args.month} progressively; raw archive will not be saved.", flush=True)
            report = extract(source, output, args.file)
    except Exception as exc:
        report = {'dataset': 'TweetsKB', 'month': args.month, 'status': 'failed',
                  'error': f'{type(exc).__name__}: {exc}'}
        report_path.parent.mkdir(exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(report, indent=2), flush=True)
        raise SystemExit(1)
    report_path.parent.mkdir(exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2), flush=True)


if __name__ == '__main__':
    main()
