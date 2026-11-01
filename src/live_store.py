"""Idempotent aggregate storage. SQLite stays on a Linux Docker volume.

The Windows dashboard reads atomic JSON snapshots, never the live SQLite file.
"""
from datetime import datetime, timezone
from pathlib import Path
import sqlite3


def connect(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path), timeout=30)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE IF NOT EXISTS hourly (hour INTEGER, hashtag TEXT, count INTEGER, batch INTEGER, PRIMARY KEY(hour,hashtag))')
    return db


def save_batch(path, rows, batch_id):
    db = connect(path)
    try:
        with db:
            buffer = []
            for row in rows:
                if row.get('kind') != 'event':
                    continue
                buffer.append((int(row['hour']), str(row['hashtag']), int(row['count']), int(batch_id)))
                if len(buffer) >= 500:
                    upsert(db, buffer)
                    buffer.clear()
            upsert(db, buffer)
    finally:
        db.close()


def upsert(db, rows):
    db.executemany('INSERT INTO hourly VALUES(?,?,?,?) ON CONFLICT(hour,hashtag) DO UPDATE SET count=excluded.count,batch=excluded.batch WHERE excluded.batch>=hourly.batch', rows)


def snapshot(path, run, watermark=None, phase='running', last_offset=None):
    db = connect(path)
    try:
        total, first, last = db.execute('SELECT COALESCE(SUM(count),0),MIN(hour),MAX(hour) FROM hourly').fetchone()
        recent = []
        if last is not None:
            rows = db.execute('SELECT hour,hashtag,count FROM hourly WHERE hour>=? ORDER BY hour,hashtag LIMIT 100001', (last - 96 * 3600,)).fetchall()
            if len(rows) > 100000:
                raise ValueError('Live export exceeds 100,000 hourly rows; choose a smaller replay interval')
            recent = [{'bucket': datetime.fromtimestamp(hour, timezone.utc).isoformat(), 'hashtag': tag, 'count': count} for hour, tag, count in rows]
        return {'source': 'TweetsKB', 'run_id': run['run_id'], 'dataset_id': run['dataset_id'],
                'phase': phase, 'updated_at': datetime.now(timezone.utc).isoformat(),
                'total_events': int(total), 'expected_events': run['expected_events'],
                'watermark': watermark, 'last_offset': last_offset,
                'last_observed_hour': datetime.fromtimestamp(last, timezone.utc).isoformat() if last is not None else None,
                'replay_start': run['replay_start'], 'replay_end_exclusive': run['replay_end_exclusive'],
                'forecast_start': run['forecast_start'], 'speed': run['speed'], 'rows': recent}
    finally:
        db.close()
