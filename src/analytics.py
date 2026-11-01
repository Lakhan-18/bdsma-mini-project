"""Chronological count forecasting. No future labels enter the deployed model."""
import argparse
from pathlib import Path
import hashlib
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from src.features import FEATURES
from src.project import ROOT, MONTH, load_events, write_json

MODEL_PATH = ROOT / 'models' / 'real' / 'next_hour.joblib'
METRICS_PATH = ROOT / 'outputs' / 'real_metrics.json'


def make_features(hourly, tags, start, end):
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    if len(tags) * len(hours) > 2000000:
        raise ValueError('Feature budget exceeded')
    values = hourly.set_index(['hashtag', 'bucket'])['count']
    pieces = []
    for tag in tags:
        counts = values.loc[tag].reindex(hours, fill_value=0) if tag in values.index.get_level_values(0) else pd.Series(0, index=hours)
        pieces.append(pd.DataFrame({'hashtag': tag, 'bucket': hours, 'count': counts.to_numpy()}))
    frame = pd.concat(pieces, ignore_index=True)
    grouped = frame.groupby('hashtag')['count']
    frame['lag_1'] = grouped.shift(1)
    frame['lag_2'] = grouped.shift(2)
    frame['mean_3'] = grouped.transform(lambda s: s.rolling(3, min_periods=3).mean())
    frame['growth'] = (frame['count'] - frame['lag_1']) / frame.lag_1.clip(lower=1)
    frame['hour'] = frame.bucket.dt.hour
    frame['target'] = grouped.shift(-1)
    frame['target_hour'] = frame.bucket + pd.Timedelta(hours=1)
    return frame


def new_forest():
    return RandomForestRegressor(n_estimators=80, max_depth=10, min_samples_leaf=3, random_state=42, n_jobs=2)


def train_frame(events, start, end, source='synthetic_test'):
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    if len(hours) < 48:
        raise ValueError('At least 48 complete source hours are required')
    validation_start, test_start = hours[int(len(hours) * .6)], hours[int(len(hours) * .8)]
    clean = events[(events.timestamp >= start) & (events.timestamp < end)].copy()
    training_tags = (clean[clean.timestamp < validation_start].groupby('hashtag').size()
                     .sort_values(ascending=False).head(120).index.tolist())
    if not training_tags:
        raise ValueError('Training period has no hashtag events')
    clean['bucket'] = clean.timestamp.dt.floor('h')
    hourly = clean.groupby(['hashtag', 'bucket']).size().rename('count').reset_index()
    features = make_features(hourly, training_tags, start, end).dropna(subset=FEATURES + ['target'])
    train = features[features.target_hour < validation_start]
    validation = features[(features.target_hour >= validation_start) & (features.target_hour < test_start)]
    test = features[features.target_hour >= test_start]
    if any(part.empty for part in [train, validation, test]):
        raise ValueError('Chronological split produced an empty partition')
    candidate = new_forest().fit(train[FEATURES], train.target)
    rf_validation = mean_absolute_error(validation.target, candidate.predict(validation[FEATURES]))
    base_validation = mean_absolute_error(validation.target, validation['count'])
    algorithm = 'Random Forest' if rf_validation <= base_validation else 'Persistence baseline'
    past = features[features.target_hour < test_start]
    forest = new_forest().fit(past[FEATURES], past.target) if algorithm == 'Random Forest' else None
    predicted = np.maximum(0, forest.predict(test[FEATURES]) if forest is not None else test['count'].to_numpy())
    metrics = {
        'source': source, 'algorithm': algorithm, 'target': 'Next-hour observed hashtag count',
        'split': '60% train / 20% validation / 20% test, chronological; labels never cross boundaries',
        'validation_start': str(validation_start), 'test_start': str(test_start),
        'training_feature_rows': len(train), 'validation_rows': len(validation), 'test_rows': len(test),
        'modeled_hashtags': len(training_tags),
        'validation_rf_mae': float(rf_validation), 'validation_persistence_mae': float(base_validation),
        'test_mae': float(mean_absolute_error(test.target, predicted)),
        'test_rmse': float(np.sqrt(mean_squared_error(test.target, predicted))),
        'test_persistence_mae': float(mean_absolute_error(test.target, test['count'])),
        'nonzero_test_rows': int((test.target > 0).sum()),
        'nonzero_test_mae': float(mean_absolute_error(test.loc[test.target > 0, 'target'], predicted[test.target.to_numpy() > 0])) if (test.target > 0).any() else None,
        'model_fit_target_end_exclusive': str(test_start),
        'note': 'The test period remains unseen by the deployed model. Sparse count data can make overall MAE look small; inspect the nonzero-event metric too.'}
    bundle = {'model': forest, 'features': FEATURES, 'source': source, 'algorithm': algorithm,
              'trained_hashtags': training_tags, 'fit_end_exclusive': str(test_start),
              'replay_start': str(test_start - pd.Timedelta(hours=3)), 'forecast_start': str(test_start),
              'replay_end_exclusive': str(end), 'coverage_start': str(start)}
    predictions = test[['hashtag', 'bucket', 'target_hour', 'count', 'target']].copy()
    predictions['prediction'] = predicted
    predictions['persistence'] = test['count'].to_numpy()
    return bundle, metrics, predictions


def train_real(month=MONTH):
    events, ingestion, engine = load_events(month)
    # Only fully bounded hours are used; first/last partial source hours are excluded.
    start = pd.Timestamp(ingestion['min_timestamp']).ceil('h')
    end = pd.Timestamp(ingestion['max_timestamp']).floor('h')
    bundle, metrics, predictions = train_frame(events, start, end, source='TweetsKB')
    from src.sources import load_source
    identity = hashlib.sha256((load_source(month)['md5'] + ':' + str(len(events))).encode()).hexdigest()
    bundle.update(month=month, dataset_id=identity)
    metrics.update(month=month, dataset_id=identity, preparation=engine,
                   total_hashtag_events=len(events), distinct_hashtags=int(events.hashtag.nunique()),
                   coverage_start=str(start), coverage_end_exclusive=str(end))
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = MODEL_PATH.with_suffix('.tmp')
    joblib.dump(bundle, temporary)
    temporary.replace(MODEL_PATH)
    (ROOT / 'data' / 'analytics').mkdir(parents=True, exist_ok=True)
    predictions.to_parquet(ROOT / 'data' / 'analytics' / 'test_predictions.parquet', index=False)
    write_json(METRICS_PATH, metrics)
    return metrics


def predict_counts(bundle, hourly, anchor, tags=None):
    anchor = pd.Timestamp(anchor)
    if anchor.tzinfo is None:
        anchor = anchor.tz_localize('UTC')
    if tags is None:
        tags = sorted(set(hourly.hashtag))
    lookup = {(str(row.hashtag), pd.Timestamp(row.bucket)): float(row.count) for row in hourly.itertuples(index=False)}
    records = []
    for tag in tags:
        current, lag1, lag2 = [lookup.get((tag, anchor - pd.Timedelta(hours=i)), 0.0) for i in range(3)]
        records.append({'hashtag': tag, 'count': current, 'lag_1': lag1, 'lag_2': lag2,
                        'mean_3': (current + lag1 + lag2) / 3, 'growth': (current - lag1) / max(lag1, 1),
                        'hour': anchor.hour, 'observed_last_3h': current + lag1 + lag2})
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    if bundle['model'] is None:
        frame['prediction'] = frame['count']
    else:
        frame['prediction'] = np.maximum(0, bundle['model'].predict(frame[FEATURES]))
    frame['forecast_hour'] = anchor + pd.Timedelta(hours=1)
    frame['seen_in_training'] = frame.hashtag.isin(bundle['trained_hashtags'])
    return frame


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--month', default=MONTH)
    args = parser.parse_args()
    print(json.dumps(train_real(args.month), indent=2))
