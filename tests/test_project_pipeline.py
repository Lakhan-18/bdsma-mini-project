"""Generated fixtures only. These tests do not publish fake real-data artifacts."""
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
import numpy as np
import pandas as pd
from src.analytics import make_features, train_frame, predict_counts
from src.features import FEATURES
from src.live_store import save_batch, snapshot
from src.project import ROOT, read_json, write_json
from src.domains import domain_for

TEST_ROOT = ROOT / 'outputs' / 'test_scratch'
TEST_ROOT.mkdir(parents=True, exist_ok=True)


class PipelineTests(unittest.TestCase):
    def test_serving_and_training_features_agree(self):
        hours = pd.date_range('2023-01-01', periods=5, freq='h', tz='UTC')
        hourly = pd.DataFrame({'hashtag': ['ai'] * 5, 'bucket': hours, 'count': [3, 0, 2, 4, 5]})
        training = make_features(hourly, ['ai'], hours[0], hours[-1] + pd.Timedelta(hours=1))
        serving = predict_counts({'model': None, 'trained_hashtags': ['ai']}, hourly, hours[3], ['ai'])
        expected = training[training.bucket == hours[3]][FEATURES].to_numpy(dtype=float)
        self.assertTrue(np.allclose(expected, serving[FEATURES].to_numpy(dtype=float)))
        self.assertEqual(serving.forecast_hour.iloc[0], hours[4])
        self.assertEqual(serving.prediction.iloc[0], 4)

    def test_chronological_model_deployment_excludes_test_targets(self):
        hours = pd.date_range('2023-01-01', periods=96, freq='h', tz='UTC')
        rows = []
        for index, stamp in enumerate(hours):
            for tag in ['ai', 'cricket']:
                for number in range(1 + index % 4):
                    rows.append({'tweet_id': f'{index}-{tag}-{number}', 'timestamp': stamp, 'hashtag': tag})
        bundle, metrics, predictions = train_frame(pd.DataFrame(rows), hours[0], hours[-1] + pd.Timedelta(hours=1))
        cutoff = pd.Timestamp(bundle['fit_end_exclusive'])
        self.assertEqual(cutoff, pd.Timestamp(metrics['test_start']))
        self.assertTrue((predictions.target_hour >= cutoff).all())
        self.assertLess(pd.Timestamp(bundle['replay_start']), cutoff)
        self.assertEqual(bundle['source'], 'synthetic_test')
        self.assertTrue(np.isfinite(metrics['test_mae']))

    def test_aggregate_upserts_are_idempotent_and_clocks_excluded(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as folder:
            database = Path(folder) / 'state.sqlite'
            hour = int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp())
            row = {'hour': hour, 'hashtag': 'ai', 'kind': 'event', 'count': 3}
            save_batch(database, [row], 1)
            save_batch(database, [row], 1)
            save_batch(database, [{**row, 'count': 5}], 2)
            save_batch(database, [{**row, 'count': 2}], 1)
            save_batch(database, [{**row, 'kind': 'clock', 'count': 100}], 3)
            run = {'run_id': 'unit', 'dataset_id': 'unit', 'expected_events': 5, 'speed': 1,
                   'replay_start': '2023-01-01T00:00:00+00:00', 'forecast_start': '2023-01-01T01:00:00+00:00',
                   'replay_end_exclusive': '2023-01-01T02:00:00+00:00'}
            result = snapshot(database, run)
            self.assertEqual(result['total_events'], 5)
            self.assertEqual(len(result['rows']), 1)
            self.assertEqual(result['rows'][0]['count'], 5)

    def test_atomic_json_round_trip(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as folder:
            path = Path(folder) / 'snapshot.json'
            write_json(path, {'phase': 'one', 'tag': 'café'})
            write_json(path, {'phase': 'two'})
            self.assertEqual(read_json(path), {'phase': 'two'})
            self.assertEqual(list(Path(folder).glob('*.tmp')), [])

    def test_domains_do_not_match_ai_inside_unrelated_words(self):
        self.assertEqual(domain_for('AI'), 'Technology')
        self.assertEqual(domain_for('train'), 'Other')
        self.assertEqual(domain_for('cricket'), 'Sports')


if __name__ == '__main__':
    unittest.main()
