"""Verify Python -> Kafka -> Spark for a bounded batch of six test records.

This is a connector smoke test, not the continuous Structured Streaming job.
It uses a dedicated test topic and explicit offsets, keeping test data separate
from the later TweetsKB ingestion topic. Run in the Java 17 project terminal.
"""
import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import socket
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spark_runtime import configure_spark_environment

TOPIC = 'bdsma-spark-smoke-test'
BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
CONNECTOR = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7'


def send_test_records(run_id):
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id='bdsma-smoke-admin',
                             request_timeout_ms=15000, api_version_auto_timeout_ms=10000)
    try:
        try:
            admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1,
                                          topic_configs={'retention.ms': '3600000'})])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()

    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', retries=3,
                             max_block_ms=15000, request_timeout_ms=15000,
                             api_version_auto_timeout_ms=10000,
                             value_serializer=lambda value: json.dumps(value).encode('utf-8'))
    offsets = []
    try:
        for index, tag in enumerate(['ai', 'cricket', 'ai', 'bollywood', 'cricket', 'ai']):
            record = {'run_id': run_id, 'source': 'synthetic',
                      'tweet_id': f'test-{run_id}-{index}', 'hashtag': tag,
                      'timestamp': datetime.now(timezone.utc).isoformat()}
            metadata = producer.send(TOPIC, partition=0, key=run_id.encode('ascii'),
                                     value=record).get(timeout=15)
            offsets.append(metadata.offset)
        producer.flush(timeout=15)
    finally:
        producer.close(timeout=5)
    return min(offsets), max(offsets) + 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--debug', action='store_true', help='Save Kafka client diagnostics to outputs/kafka_client.log')
    args = parser.parse_args()
    if args.debug:
        log_path = ROOT / 'outputs' / 'kafka_client.log'
        log_path.parent.mkdir(exist_ok=True)
        handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
        logger = logging.getLogger('kafka')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.propagate = False
        print(f'Kafka diagnostic log: {log_path}', flush=True)
    spark = None
    run_id = uuid.uuid4().hex
    report = {'test': 'kafka_spark_batch_connector', 'source': 'synthetic',
              'status': 'failed', 'run_id': run_id, 'topic': TOPIC,
              'bootstrap': BOOTSTRAP, 'stage': 'python_tcp_connection'}
    print('TEST DATA ONLY: Python -> Kafka -> Spark (bounded batch).', flush=True)
    print('Keep Docker Desktop running. No TweetsKB download is performed.', flush=True)
    try:
        host, port = BOOTSTRAP.rsplit(':', 1)
        with socket.create_connection((host, int(port)), timeout=5):
            pass
        print(f'Python TCP connection to {BOOTSTRAP}: OK (Kafka handshake not yet checked).', flush=True)
        configure_spark_environment()
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, from_json, lit

        report['stage'] = 'kafka_api_and_producer'
        start, end = send_test_records(run_id)
        print(f'PASS 1/2: Kafka acknowledged six records in {TOPIC}.', flush=True)
        print('Loading the Spark Kafka connector. First run may download Maven dependencies.', flush=True)
        report['stage'] = 'spark_connector_startup'
        spark = (SparkSession.builder.master('local[2]')
                 .appName('BDSMA-Kafka-Spark-Connector-Test')
                 .config('spark.jars.packages', CONNECTOR)
                 .config('spark.driver.memory', '1g')
                 .config('spark.driver.maxResultSize', '16m')
                 .config('spark.sql.shuffle.partitions', '2')
                 .config('spark.ui.enabled', 'false')
                 .getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        if spark.version != '3.5.7':
            raise RuntimeError(f'Expected Spark 3.5.7 for the pinned connector, found {spark.version}')
        report['stage'] = 'spark_kafka_read'
        raw = (spark.read.format('kafka')
               .option('kafka.bootstrap.servers', BOOTSTRAP)
               .option('assign', json.dumps({TOPIC: [0]}))
               .option('startingOffsets', json.dumps({TOPIC: {'0': start}}))
               .option('endingOffsets', json.dumps({TOPIC: {'0': end}}))
               .option('kafka.request.timeout.ms', '15000')
               .option('kafka.default.api.timeout.ms', '30000')
               .option('failOnDataLoss', 'true')
               .load())
        parsed = raw.select(from_json(col('value').cast('string'),
                                      'run_id string, hashtag string').alias('event'))
        events = parsed.select('event.*').where(col('run_id') == lit(run_id))
        counts = events.groupBy('hashtag').count()
        actual = {row.hashtag: row['count'] for row in counts.collect()}
        expected = {'ai': 3, 'cricket': 2, 'bollywood': 1}
        if actual != expected:
            raise AssertionError(f'Kafka/Spark count mismatch: expected {expected}, got {actual}')
        print(json.dumps(actual, indent=2), flush=True)
        print('PASS 2/2: Spark read Kafka and produced the expected hashtag counts.', flush=True)
        report.update(status='passed', stage='complete', spark_version=spark.version, hashtag_counts=actual,
                      starting_offset=start, ending_offset_exclusive=end)
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            output = ROOT / 'outputs' / 'kafka_spark_smoke.json'
            output.parent.mkdir(exist_ok=True)
            output.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print('KAFKA-SPARK CONNECTOR TEST PASSED.', flush=True)
    print('Continuous streaming, checkpoints and real TweetsKB ingestion remain separate steps.', flush=True)


if __name__ == '__main__':
    main()
