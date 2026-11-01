"""Shared paths, atomic snapshots and verified real-data loading."""
from pathlib import Path
import json
import os
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
MONTH = '2019-10'
RUNTIME = ROOT / 'outputs' / 'runtime'
TOPIC = 'bdsma-hashtag-events'


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {} if default is None else default


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(.05)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_events(month=MONTH):
    import pandas as pd
    import pyarrow.dataset as ds
    report = read_json(ROOT / 'outputs' / f'ingest_{month}.json')
    if report.get('status') != 'complete' or report.get('publisher_md5_verified') is not True:
        raise ValueError('A complete, checksum-verified TweetsKB extraction is required')
    lake = ROOT / 'data' / 'lake' / month
    prepared = read_json(ROOT / 'outputs' / f'preprocess_{month}.json')
    if lake.exists() and prepared.get('status') == 'complete':
        source, engine = lake, 'PySpark Parquet lake'
    else:
        source, engine = ROOT / 'data' / 'extracted' / month / 'events.parquet', 'Verified extraction (Spark preprocessing pending)'
    dataset = ds.dataset(source, format='parquet', exclude_invalid_files=True)
    if dataset.count_rows() > 500000:
        raise ValueError('The laptop model/dashboard is limited to 500,000 events per prepared demo; use a bounded interval for larger months')
    frame = dataset.to_table(columns=['tweet_id', 'timestamp', 'hashtag']).to_pandas()
    frame['timestamp'] = pd.to_datetime(frame.timestamp, utc=True)
    frame = frame.drop_duplicates(['tweet_id', 'hashtag']).sort_values(['timestamp', 'tweet_id', 'hashtag']).reset_index(drop=True)
    return frame, report, engine
