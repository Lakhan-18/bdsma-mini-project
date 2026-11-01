import unittest
import pandas as pd
from src.features import hourly_features


class FeaturesTests(unittest.TestCase):
    def test_zero_hours_lags_and_future_target(self):
        frame = hourly_features(pd.DataFrame([
            {'tweet_id': '1', 'hashtag': '#AI', 'timestamp': '2023-01-01T00:00:00Z'},
            {'tweet_id': '2', 'hashtag': 'ai', 'timestamp': '2023-01-01T02:00:00Z'},
            {'tweet_id': '3', 'hashtag': 'sport', 'timestamp': '2023-01-01T02:00:00Z'},
        ]))
        ai = frame[frame.hashtag == 'ai'].reset_index(drop=True)
        self.assertEqual(ai['count'].tolist(), [1, 0, 1])
        self.assertEqual(ai.target.iloc[:2].tolist(), [0, 1])
        self.assertEqual(ai.lag_1.iloc[2], 0)
        self.assertEqual(ai.lag_2.iloc[2], 1)
        self.assertAlmostEqual(ai.mean_3.iloc[2], 2 / 3)
        self.assertTrue(pd.isna(ai.target.iloc[2]))

    def test_same_tweet_tag_is_counted_once(self):
        row = {'tweet_id': '1', 'hashtag': '#AI', 'timestamp': '2023-01-01T00:00:00Z'}
        frame = hourly_features(pd.DataFrame([row, row, {**row, 'tweet_id': '2'}]))
        self.assertEqual(frame['count'].tolist(), [2])

    def test_null_and_empty_tags_are_excluded(self):
        rows = [{'hashtag': tag, 'timestamp': '2023-01-01T00:00:00Z'} for tag in [None, '', '#', ' AI ']]
        self.assertEqual(hourly_features(pd.DataFrame(rows)).hashtag.tolist(), ['ai'])

    def test_memory_limit_is_checked_before_expansion(self):
        rows = [{'hashtag': 'ai', 'timestamp': timestamp} for timestamp in ['2023-01-01', '2023-01-02']]
        with self.assertRaisesRegex(ValueError, 'memory limit'):
            hourly_features(pd.DataFrame(rows), max_cells=10)


if __name__ == '__main__':
    unittest.main()
