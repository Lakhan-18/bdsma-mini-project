"""Validate and deduplicate the extracted month into the PySpark Parquet lake.

Run using the Compose preprocess service, which shares the verified Spark image.
This is batch preparation; it neither replays Kafka records nor trains a model.
"""
import argparse
import json
from pathlib import Path
import re
import shutil
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spark_runtime import configure_spark_environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month', default='2013-02')
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9]{4}-(?:0[1-9]|1[0-2])', args.month):
        parser.error('Expected month in YYYY-MM format')
    input_path = ROOT / 'data' / 'extracted' / args.month / 'events.parquet'
    lake_root = ROOT / 'data' / 'lake'
    output = lake_root / args.month
    stage = lake_root / f'{args.month}-{uuid.uuid4().hex}.partial'
    report_path = ROOT / 'outputs' / f'preprocess_{args.month}.json'
    if output.exists():
        print(f'Lake output already exists: {output}; no files were overwritten.')
        return
    spark = None
    events = None
    stage_owned = False
    report = {'dataset': 'TweetsKB', 'month': args.month, 'status': 'failed'}
    try:
        ingestion = json.loads((ROOT / 'outputs' / f'ingest_{args.month}.json').read_text(encoding='utf-8'))
        if ingestion.get('status') != 'complete' or ingestion.get('publisher_md5_verified') is not True:
            raise ValueError('Complete, integrity-verified real-data ingestion is required first')
        if not input_path.is_file() or input_path.stat().st_size != ingestion['parquet_bytes']:
            raise ValueError('Extracted Parquet file is missing or does not match the ingestion report')
        lake_root.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(lake_root).free < 2 * 1024**3 + 3 * input_path.stat().st_size:
            raise ValueError('Insufficient space for staging and a 2 GiB free-space reserve')
        configure_spark_environment()
        from pyspark.sql import SparkSession, functions as F

        print('Starting real TweetsKB PySpark preprocessing...', flush=True)
        spark = (SparkSession.builder.master('local[2]').appName('BDSMA-TweetsKB-Preprocessing')
                 .config('spark.driver.memory', '1g')
                 .config('spark.sql.shuffle.partitions', '2')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.ui.enabled', 'false').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        raw = spark.read.parquet(str(input_path)).select('tweet_id', 'timestamp', 'hashtag')
        invalid = raw.where(F.col('tweet_id').isNull() | ~F.col('tweet_id').rlike('^[0-9]+$') |
                            F.col('timestamp').isNull() | F.col('hashtag').isNull() |
                            (F.length(F.trim('hashtag')) == 0))
        if invalid.limit(1).count():
            raise ValueError('Invalid required fields found; no records were silently discarded')
        if (raw.groupBy('tweet_id').agg(F.countDistinct('timestamp').alias('timestamps'))
                .where(F.col('timestamps') > 1).limit(1).count()):
            raise ValueError('One tweet ID has conflicting timestamps; inspect source before deduplicating')
        before = raw.count()
        events = raw.dropDuplicates(['tweet_id', 'hashtag']).withColumn('event_date', F.to_date('timestamp')).cache()
        after = events.count()
        if not after:
            raise ValueError('No valid hashtag events to publish')
        bounds = events.agg(F.min('timestamp').alias('first'), F.max('timestamp').alias('last')).first()
        distinct_tags = events.select('hashtag').distinct().count()
        top = [row.asDict() for row in events.groupBy('hashtag').count().orderBy(F.desc('count'), 'hashtag').limit(10).collect()]
        # Stage under a unique owned directory, then publish after a row-count check.
        stage.mkdir()
        stage_owned = True
        events.coalesce(2).write.mode('overwrite').partitionBy('event_date').parquet(str(stage))
        written = spark.read.parquet(str(stage)).count()
        if written != after:
            raise AssertionError(f'Parquet round-trip count mismatch: {written} != {after}')
        if shutil.disk_usage(lake_root).free < 2 * 1024**3:
            raise ValueError('Stopped before publishing to preserve free disk space')
        stage.rename(output)
        stage_owned = False
        report.update(status='complete', source='tweetskb', input_rows=before, output_rows=after,
                      duplicate_pairs_removed=before - after, distinct_hashtags=distinct_tags,
                      first_event_utc=bounds['first'].isoformat() + 'Z',
                      last_event_utc=bounds['last'].isoformat() + 'Z', top_hashtags=top,
                      parquet_bytes=sum(path.stat().st_size for path in output.rglob('*.parquet')),
                      source_archive_md5=load_md5(args.month), output=str(output))
        print('REAL-DATA PREPROCESSING PASSED.', flush=True)
        print(json.dumps(report, indent=2), flush=True)
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        try:
            if events is not None:
                events.unpersist()
            if spark is not None:
                spark.stop()
        finally:
            # Remove only this invocation's unique staging directory in the lake.
            if stage_owned and stage.exists() and stage.resolve().parent == lake_root.resolve():
                shutil.rmtree(stage)
            report_path.parent.mkdir(exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')


def load_md5(month):
    from src.sources import load_source
    return load_source(month)['md5']


if __name__ == '__main__':
    main()
