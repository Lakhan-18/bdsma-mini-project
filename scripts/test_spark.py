"""Check local Spark, Python workers and hashtag aggregation without real data.

Run from the project terminal after dot-sourcing Setup.ps1 -CheckOnly.
This test neither downloads TweetsKB nor connects to Kafka.
"""
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spark_runtime import configure_spark_environment


def main():
    configure_spark_environment()
    from pyspark.sql import SparkSession

    print('TEST DATA ONLY: no TweetsKB data is used.', flush=True)
    print(f'Python: {sys.executable}', flush=True)
    print(f'JAVA_HOME: {os.environ.get("JAVA_HOME", "not set")}', flush=True)
    print('Starting Spark in local mode with two worker threads...', flush=True)
    spark = None
    report = {'test': 'local_spark', 'source': 'synthetic', 'status': 'failed'}
    report_path = ROOT / 'outputs' / 'spark_smoke.json'
    try:
        spark = (SparkSession.builder
                 .master('local[2]')
                 .appName('BDSMA-Local-Spark-Test')
                 .config('spark.driver.memory', '1g')
                 .config('spark.driver.maxResultSize', '16m')
                 .config('spark.sql.shuffle.partitions', '2')
                 .config('spark.ui.enabled', 'false')
                 .getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')

        if spark.range(10).count() != 10:
            raise AssertionError('Spark JVM count check failed')
        print('PASS 1/3: Spark started and counted 10 rows.', flush=True)

        doubled = sorted(spark.sparkContext.parallelize([1, 2, 3], 2)
                         .map(lambda value: value * 2).collect())
        if doubled != [2, 4, 6]:
            raise AssertionError(f'Python worker check failed: {doubled}')
        print('PASS 2/3: Python workers returned [2, 4, 6].', flush=True)

        rows = [('ai',), ('cricket',), ('ai',), ('bollywood',), ('cricket',), ('ai',)]
        counts = spark.createDataFrame(rows, 'hashtag string').groupBy('hashtag').count()
        actual = {row.hashtag: row['count'] for row in counts.collect()}
        expected = {'ai': 3, 'cricket': 2, 'bollywood': 1}
        if actual != expected:
            raise AssertionError(f'Hashtag count check failed: {actual}')
        counts.orderBy('count', ascending=False).show(truncate=False)
        print('PASS 3/3: Hashtag aggregation matches the expected counts.', flush=True)
        report.update(status='passed', spark_version=spark.version,
                      python_worker_output=doubled, hashtag_counts=actual)
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            report_path.parent.mkdir(exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print('SPARK TEST PASSED. Kafka connector testing is still a separate step.', flush=True)


if __name__ == '__main__':
    main()
