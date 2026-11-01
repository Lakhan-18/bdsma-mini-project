"""Hourly counts and features shared by training and forecasting."""
import pandas as pd

FEATURES = ['count', 'lag_1', 'lag_2', 'mean_3', 'growth', 'hour']


def hourly_features(events, max_cells=2_000_000):
    events = events.copy()
    events['timestamp'] = pd.to_datetime(events['timestamp'], utc=True, errors='raise')
    events = events.dropna(subset=['timestamp', 'hashtag'])
    events['hashtag'] = events['hashtag'].astype(str).str.strip().str.lstrip('#').str.casefold()
    events = events[events['hashtag'].str.len() > 0]
    if events.empty:
        raise ValueError('No hashtag events found')
    events['bucket'] = events.timestamp.dt.floor('h')
    if 'tweet_id' in events:
        events = events.drop_duplicates(subset=['tweet_id', 'hashtag'])
    counts = events.groupby(['hashtag', 'bucket']).size().rename('count')
    # Fill absent hours, including hours after a hashtag disappears.
    hours = pd.date_range(events.bucket.min(), events.bucket.max(), freq='h')
    tags = sorted(events.hashtag.unique())
    if len(tags) * len(hours) > max_cells:
        raise ValueError('Offline feature table exceeds the memory limit. Select a smaller time range/hashtag set or use the planned Spark aggregation.')
    # Build one hashtag at a time instead of materializing the product index.
    pieces = []
    for tag in tags:
        series = counts.loc[tag].reindex(hours, fill_value=0)
        pieces.append(pd.DataFrame({'hashtag': tag, 'bucket': hours, 'count': series.to_numpy()}))
    frame = pd.concat(pieces, ignore_index=True)
    group = frame.groupby('hashtag', sort=False)['count']
    frame['lag_1'] = group.shift(1)
    frame['lag_2'] = group.shift(2)
    frame['mean_3'] = group.transform(lambda x: x.rolling(3, min_periods=3).mean())
    frame['growth'] = (frame['count'] - frame['lag_1']) / frame['lag_1'].clip(lower=1)
    frame['hour'] = frame.bucket.dt.hour
    frame['target'] = group.shift(-1)
    return frame
