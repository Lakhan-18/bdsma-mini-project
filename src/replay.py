"""Chronological replay of verified real events. No live Twitter API is used."""
from datetime import datetime, timezone
import json
import os
import time
import uuid
import joblib
import pandas as pd
from src.analytics import MODEL_PATH
from src.project import ROOT, RUNTIME, TOPIC, load_events, write_json

BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')


def ensure_topic():
    from kafka import KafkaConsumer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id='bdsma-project',
                            request_timeout_ms=15000, api_version_auto_timeout_ms=10000)
    try:
        try:
            admin.create_topics([NewTopic(TOPIC, 1, 1, topic_configs={'retention.ms': '86400000'})])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, request_timeout_ms=15000,
                             api_version_auto_timeout_ms=10000)
    try:
        if consumer.partitions_for_topic(TOPIC) != {0}:
            raise ValueError('This ordered laptop demo requires one Kafka partition')
        partition = TopicPartition(TOPIC, 0)
        return int(consumer.end_offsets([partition])[partition])
    finally:
        consumer.close()


def prepare_run(speed=14400):
    if speed <= 0:
        raise ValueError('Replay speed must be positive')
    bundle = joblib.load(MODEL_PATH)
    if bundle.get('source') != 'TweetsKB':
        raise ValueError('A real-data model is required')
    events, _, engine = load_events(bundle['month'])
    start, end = pd.Timestamp(bundle['replay_start']), pd.Timestamp(bundle['replay_end_exclusive'])
    events = events[(events.timestamp >= start) & (events.timestamp < end)].copy()
    if events.empty:
        raise ValueError('No events in the unseen replay interval')
    run = {'run_id': uuid.uuid4().hex, 'dataset_id': bundle['dataset_id'], 'source': 'TweetsKB',
           'month': bundle['month'], 'topic': TOPIC, 'starting_offset': ensure_topic(),
           'replay_start': str(start), 'replay_end_exclusive': str(end),
           'forecast_start': bundle['forecast_start'], 'expected_events': len(events),
           'speed': speed, 'preparation': engine, 'created_at': datetime.now(timezone.utc).isoformat()}
    write_json(RUNTIME / 'active_run.json', run)
    write_json(RUNTIME / 'replay.json', {'run_id': run['run_id'], 'phase': 'waiting_for_stream', 'sent': 0, 'expected': len(events)})
    return run, events


def replay_events(run, events):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', enable_idempotence=True,
                             request_timeout_ms=30000, max_block_ms=30000,
                             value_serializer=lambda value: json.dumps(value).encode('utf-8'))
    status = {'run_id': run['run_id'], 'phase': 'running', 'sent': 0, 'expected': len(events)}
    began = time.monotonic()
    source_start = pd.Timestamp(run['replay_start'])
    pending = []
    last_update = 0
    try:
        for row in events.itertuples(index=False):
            due = (row.timestamp - source_start).total_seconds() / run['speed']
            delay = began + due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            message = {'run_id': run['run_id'], 'kind': 'event', 'source': 'TweetsKB',
                       'tweet_id': str(row.tweet_id), 'timestamp': row.timestamp.isoformat(), 'hashtag': str(row.hashtag)}
            pending.append(producer.send(TOPIC, partition=0, key=str(row.hashtag).encode('utf-8'), value=message))
            if len(pending) >= 500:
                for future in pending:
                    future.get(timeout=30)
                status['sent'] += len(pending)
                pending.clear()
            if time.monotonic() - last_update >= 1:
                status.update(last_event_time=row.timestamp.isoformat(), updated_at=datetime.now(timezone.utc).isoformat())
                write_json(RUNTIME / 'replay.json', status)
                last_update = time.monotonic()
        for future in pending:
            future.get(timeout=30)
        status['sent'] += len(pending)
        # This explicit control record advances event-time watermark after replay.
        # It is counted separately by kind and never appears in hashtag totals.
        clock = pd.Timestamp(run['replay_end_exclusive']) + pd.Timedelta(hours=2)
        final = producer.send(TOPIC, partition=0, key=b'clock', value={
            'run_id': run['run_id'], 'kind': 'clock', 'source': 'control',
            'tweet_id': 'clock', 'timestamp': clock.isoformat(), 'hashtag': '__clock__'}).get(timeout=30)
        producer.flush(timeout=30)
        status.update(phase='complete', final_offset=int(final.offset),
                      elapsed_seconds=round(time.monotonic() - began, 2), updated_at=datetime.now(timezone.utc).isoformat())
        write_json(RUNTIME / 'replay.json', status)
    except Exception as exc:
        status.update(phase='failed', error=f'{type(exc).__name__}: {exc}')
        write_json(RUNTIME / 'replay.json', status)
        raise
    finally:
        producer.close(timeout=10)
    return status
