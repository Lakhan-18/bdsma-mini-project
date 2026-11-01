"""Bounded TweetsKB source inspection and optional small-file download.

Uses only the Python standard library. Inspect reads a short decompressed prefix
over HTTP; it does not save the raw archive. This is source preparation, not an
RDF-to-event parser. A successful prefix inspection does NOT verify the archive
checksum, full coverage, or the syntax of the entire file.
"""
import argparse
import codecs
from contextlib import contextmanager
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MIB = 1024 * 1024


class ByteLimitExceeded(ValueError):
    pass


class LimitedReader:
    def __init__(self, stream, maximum):
        self.stream = stream
        self.maximum = maximum
        self.count = 0

    def read(self, size=-1):
        # Allow one extra byte to detect overflow; never read an unbounded body.
        size = min(size if size >= 0 else self.maximum + 1,
                   self.maximum - self.count + 1)
        chunk = self.stream.read(size)
        self.count += len(chunk)
        if self.count > self.maximum:
            raise ByteLimitExceeded(f'Source exceeded the {self.maximum / MIB:.1f} MiB byte limit')
        return chunk


def load_source(month):
    manifest = json.loads((ROOT / 'config' / 'sources.json').read_text(encoding='utf-8'))
    for source in manifest['sources']:
        if source['month'] == month:
            return source
    raise ValueError(f'Month {month} is not in the verified source manifest')


@contextmanager
def open_source(source, local_file=None, timeout=20):
    if local_file:
        with Path(local_file).open('rb') as stream:
            yield stream, {'transport': 'local', 'location': str(Path(local_file).resolve())}
    else:
        request = urllib.request.Request(source['url'], headers={
            'User-Agent': 'BDSMA-Hashtag-Research/0.1', 'Accept-Encoding': 'identity'})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            yield response, {'transport': 'http', 'location': response.geturl(),
                             'content_length': response.headers.get('Content-Length')}


def inspect_source(source, local_file=None, preview_bytes=32768, maximum_bytes=2 * MIB):
    with open_source(source, local_file) as (stream, metadata):
        limited = LimitedReader(stream, maximum_bytes)
        with gzip.GzipFile(fileobj=limited) as decoded:
            prefix = decoded.read(preview_bytes)
        metadata['compressed_bytes_read'] = limited.count
    # A bounded prefix may stop midway through a UTF-8 code point.
    text = codecs.getincrementaldecoder('utf-8-sig')('strict').decode(prefix, final=False)
    # The last line may be cut in the middle of a statement or literal.
    lines = text.splitlines()[:40]
    return {'dataset': 'TweetsKB', 'month': source['month'],
            'status': 'prefix_inspected_only', 'source': metadata,
            'preview': lines,
            'next_step': 'Verify the serialization and implement the RDF adapter against this preview.'}


def checksum(path):
    digest = hashlib.md5()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(MIB), b''):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_source(source, destination, maximum_bytes=20 * MIB, reserve_bytes=2 * 1024**3):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if checksum(destination) != source['md5']:
            raise ValueError('Existing file has the wrong checksum; choose another output path')
        return {'status': 'already_verified', 'bytes': destination.stat().st_size}
    partial = destination.with_name(destination.name + '.partial')
    if partial.exists():
        raise ValueError(f'Partial file already exists: {partial}. Inspect it before retrying.')
    if shutil.disk_usage(destination.parent).free < maximum_bytes + reserve_bytes:
        raise ValueError('Insufficient free space for the download and 2 GiB reserve')
    digest = hashlib.md5()
    total = 0
    partial_owned = False
    try:
        with open_source(source) as (stream, metadata):
            length = metadata.get('content_length')
            if length and int(length) > maximum_bytes:
                raise ByteLimitExceeded('Server reports an archive larger than the download limit')
            limited = LimitedReader(stream, maximum_bytes)
            with partial.open('xb') as output:
                partial_owned = True
                while True:
                    chunk = limited.read(65536)
                    if not chunk:
                        break
                    if shutil.disk_usage(destination.parent).free < reserve_bytes + len(chunk):
                        raise ValueError('Stopped to preserve the free-space reserve')
                    output.write(chunk)
                    digest.update(chunk)
                    total += len(chunk)
        if digest.hexdigest() != source['md5']:
            raise ValueError('Downloaded archive failed its publisher MD5 integrity check')
        partial.rename(destination)
    except Exception:
        # Only remove the exact temporary file owned by this invocation.
        if partial_owned and partial.exists():
            partial.unlink()
        raise
    return {'status': 'downloaded_and_verified', 'bytes': total,
            'path': str(destination.resolve())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['inspect', 'fetch'])
    parser.add_argument('--month', default='2013-02')
    parser.add_argument('--file', help='Inspect a local .n3.gz archive instead of using HTTP')
    args = parser.parse_args()
    source = load_source(args.month)
    report_path = ROOT / 'outputs' / 'source_inspection.json'
    try:
        if args.action == 'inspect':
            report = inspect_source(source, args.file)
        else:
            if args.file:
                parser.error('--file applies to inspect only')
            report_path = ROOT / 'outputs' / 'source_download.json'
            report = fetch_source(source, ROOT / 'data' / 'raw' / source['filename'])
    except (OSError, ValueError, EOFError, urllib.error.URLError) as exc:
        report = {'status': 'failed', 'month': args.month,
                  'error': f'{type(exc).__name__}: {exc}',
                  'next_step': 'Check internet access from a regular Windows terminal, or inspect a locally downloaded archive with --file.'}
        report_path.parent.mkdir(exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(report, indent=2))
        raise SystemExit(1)
    report_path.parent.mkdir(exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
