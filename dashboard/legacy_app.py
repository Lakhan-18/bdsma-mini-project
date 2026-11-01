"""Archived initial synthetic-data prototype. Not used by the project launcher."""
from pathlib import Path
import json
import joblib
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title='Hashtag Trend Prediction', layout='wide')
st.title('Real-Time Hashtag Trend Prediction')
st.caption('TweetsKB → PySpark → Parquet → Kafka replay → streaming → ML')
st.info('Streaming is not connected yet. This first milestone displays an offline dataset.')
source = st.sidebar.text_input('Normalized Parquet path', 'data/fixtures/synthetic.parquet')
if not Path(source).is_file():
    st.warning('No normalized data yet. Run preprocessing or generate the synthetic fixture.')
    st.stop()
events = pd.read_parquet(source)
events['timestamp'] = pd.to_datetime(events.timestamp, utc=True)
events['hashtag'] = events.hashtag.str.lstrip('#').str.casefold()
if 'fixtures' in Path(source).parts:
    st.warning('SYNTHETIC TEST DATA — not TweetsKB results.')
hours = st.sidebar.slider('Recent hours', 1, 96, 24)
end = events.timestamp.max().floor('h') + pd.Timedelta(hours=1)
recent = events[events.timestamp >= end - pd.Timedelta(hours=hours)]
ranking = recent.groupby('hashtag').size().sort_values(ascending=False).rename('mentions')
st.subheader('Most frequent hashtags')
st.dataframe(ranking, use_container_width=True)
tag = st.selectbox('Search or select a hashtag', sorted(events.hashtag.unique()))
series = recent[recent.hashtag == tag].set_index('timestamp').resample('h').size().rename('mentions').reset_index()
st.plotly_chart(px.line(series, x='timestamp', y='mentions', title=f'#{tag} activity'), use_container_width=True)
if Path('outputs/metrics.json').exists():
    st.subheader('Chronological validation')
    st.json(json.loads(Path('outputs/metrics.json').read_text()))
st.caption('Next milestone: real TweetsKB ingestion, Kafka replay, streaming status and live forecasts.')
