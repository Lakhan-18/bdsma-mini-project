"""Synthetic fixture for plumbing checks only. This is NOT TweetsKB."""
from pathlib import Path
import random
import pandas as pd

random.seed(42)
rows = []
for timestamp in pd.date_range('2023-01-01', periods=96, freq='h', tz='UTC'):
    for tag in ['cricket', 'ai', 'bollywood', 'elections']:
        for _ in range(random.randint(2, 20)):
            rows.append({'timestamp': timestamp, 'hashtag': tag})
Path('data/fixtures').mkdir(parents=True, exist_ok=True)
pd.DataFrame(rows).to_parquet('data/fixtures/synthetic.parquet', index=False)
print('Created synthetic fixture. Not real TweetsKB data.')
