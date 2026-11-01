"""Train a next-hour count model; evaluate on later timestamps only."""
import argparse
from pathlib import Path
import json
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from src.features import FEATURES, hourly_features


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Normalized event Parquet file')
    parser.add_argument('--source', required=True, choices=['tweetskb', 'synthetic'])
    args = parser.parse_args()
    frame = hourly_features(pd.read_parquet(args.input)).dropna(subset=FEATURES + ['target'])
    times = sorted(frame.bucket.unique())
    if len(times) < 10:
        raise ValueError('Need at least 10 complete feature hours; use a longer dataset')
    cutoff = times[int(len(times) * .8)]
    # Purge the training boundary: its next-hour label must precede validation.
    train = frame[frame.bucket + pd.Timedelta(hours=1) < cutoff]
    test = frame[frame.bucket >= cutoff]
    model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=2)
    model.fit(train[FEATURES], train.target)
    metrics = {'source': args.source, 'train_rows': len(train), 'test_rows': len(test),
               'validation_start': str(cutoff),
               'model_mae': mean_absolute_error(test.target, model.predict(test[FEATURES])),
               'persistence_mae': mean_absolute_error(test.target, test['count'])}
    Path('models').mkdir(exist_ok=True)
    Path('outputs').mkdir(exist_ok=True)
    joblib.dump({'model': model, 'features': FEATURES, 'source': args.source}, 'models/next_hour.joblib')
    Path('outputs/metrics.json').write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))


if __name__ == '__main__':
    main()
