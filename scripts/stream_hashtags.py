"""Kafka Structured Streaming -> event-time windows -> idempotent snapshots."""
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.project import RUNTIME, read_json, write_json
from src.spark_runtime import configure_spark_environment
from src.live_store import save_batch, snapshot


def main():
    run = read_json(RUNTIME / 'active_run.json')
    if not run.get('run_id') or run.get('source') != 'TweetsKB':
        raise ValueError('Start a real replay through Start-Project.ps1 first')
    configure_spark_environment()
    from pyspark.sql import SparkSession, functions as F
    state = Path(os.environ.get('BDSMA_STATE_DIR', '/state')) / run['run_id']
    state.mkdir(parents=True, exist_ok=True)
    database = state / 'hourly.sqlite'
    query = spark = None
    status_path = RUNTIME / 'live.json'
    phase = 'starting'
    try:
        spark = (SparkSession.builder.master('local[2]').appName('BDSMA-Hashtag-Streaming')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.shuffle.partitions', '2')
                 .config('spark.sql.streaming.noDataMicroBatches.enabled', 'true')
                 .config('spark.ui.enabled', 'false').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        import json
        stream = (spark.readStream.format('kafka')
                  .option('kafka.bootstrap.servers', os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:19092'))
                  .option('assign', json.dumps({run['topic']: [0]}))
                  .option('startingOffsets', json.dumps({run['topic']: {'0': run['starting_offset']}}))
                  .option('maxOffsetsPerTrigger', 5000).option('failOnDataLoss', 'true').load())
        schema = 'run_id string, kind string, source string, tweet_id string, timestamp string, hashtag string'
        events = (stream.select(F.from_json(F.col('value').cast('string'), schema).alias('event'))
                  .select('event.*').where(F.col('run_id') == run['run_id'])
                  .withColumn('timestamp', F.to_timestamp('timestamp'))
                  .where(F.col('timestamp').isNotNull() & F.col('hashtag').isNotNull() & F.col('kind').isin('event', 'clock')))
        counts = (events.withWatermark('timestamp', '10 minutes')
                  .groupBy(F.window('timestamp', '1 hour'), 'hashtag', 'kind').count())

        def update(batch, batch_id):
            rows = batch.select(F.col('window.start').cast('long').alias('hour'), 'hashtag', 'kind', 'count')
            save_batch(database, (row.asDict() for row in rows.toLocalIterator()), batch_id)

        query = (counts.writeStream.outputMode('update').foreachBatch(update)
                 .option('checkpointLocation', str(state / 'checkpoint'))
                 .trigger(processingTime='2 seconds').start())
        phase = 'running'
        print('STREAM READY: waiting for chronological TweetsKB replay.', flush=True)
        while query.isActive:
            progress = query.lastProgress or {}
            watermark = progress.get('eventTime', {}).get('watermark')
            sources = progress.get('sources', [])
            ending = sources[0].get('endOffset', {}).get(run['topic'], {}).get('0') if sources else None
            current = snapshot(database, run, watermark, phase, ending)
            replay = read_json(RUNTIME / 'replay.json')
            if replay.get('run_id') == run['run_id'] and replay.get('phase') == 'failed':
                raise RuntimeError('Producer failed: ' + replay.get('error', 'unknown error'))
            if replay.get('phase') == 'complete' and replay.get('run_id') == run['run_id']:
                end = datetime.fromisoformat(run['replay_end_exclusive'])
                mark = datetime.fromisoformat(watermark.replace('Z', '+00:00')) if watermark else None
                if (current['total_events'] == run['expected_events'] and mark and mark >= end and
                        ending is not None and int(ending) > replay['final_offset']):
                    phase = 'complete'
                    current['phase'] = phase
                    write_json(status_path, current)
                    print('STREAMING REPLAY VERIFIED: all real events processed and windows finalized.', flush=True)
                    break
            write_json(status_path, current)
            query.awaitTermination(1)
        if phase != 'complete':
            error = query.exception() if query else None
            raise RuntimeError(f'Stream stopped before completion: {error}')
    except Exception as exc:
        write_json(status_path, {'run_id': run['run_id'], 'source': 'TweetsKB', 'phase': 'failed',
                                'error': f'{type(exc).__name__}: {exc}', 'updated_at': datetime.now(timezone.utc).isoformat()})
        raise
    finally:
        if query is not None:
            query.stop()
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
