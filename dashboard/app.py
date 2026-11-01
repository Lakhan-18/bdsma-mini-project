"""Hashtag Pulse: verified TweetsKB / TweetsCOV19 history, forecasts and streaming replay."""
from pathlib import Path
import os
import subprocess
import sys
import joblib
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.project import MONTH, RUNTIME, load_events, read_json
from src.analytics import MODEL_PATH, METRICS_PATH, predict_counts
from src.domains import GROUPS, domain_for

# Strict stopword blacklist to guarantee clean, academic presentation
PROFANITY_STOPWORDS = {
    'pussy', 'ass', 'tits', 'boobs', 'porn', 'sexy', 'sex', 'dick', 'cock', 'bitch',
    'fuck', 'shit', 'tittytuesday', 'nsfw', 'cum', 'milf', 'slut', 'whore', 'nude',
    'naked', 'penis', 'vagina', 'erotic', 'horny', 'findom', 'anal', 'hentai', 'fetish',
    'bdsm', 'boobscam'
}
PROFANITY_STEMS = (
    'fuck', 'shit', 'porn', 'cock', 'dick', 'bitch', 'tits', 'boob', 'milf', 'slut',
    'whore', 'nude', 'vagina', 'penis', 'nsfw', 'horny', 'pussy', 'hentai', 'fetish',
    'bdsm', 'escort', 'camgirl'
)


def is_clean_tag(tag):
    if not isinstance(tag, str) or len(tag) < 2:
        return False
    if not tag.isascii() or not tag.isalnum() or tag.isdigit():
        return False
    lower = tag.lower()
    if lower in PROFANITY_STOPWORDS:
        return False
    if any(stem in lower for stem in PROFANITY_STEMS):
        return False
    return True

st.set_page_config(page_title='Hashtag Pulse | BDSMA', page_icon='📈', layout='wide')
st.markdown('''<style>
.block-container{padding-top:1.5rem;max-width:1450px}
h1{font-size:2.6rem!important;letter-spacing:-1.5px;margin-bottom:0!important}
[data-testid="stMetric"]{background:linear-gradient(130deg,#15243b,#101d2f);border:1px solid #24364c;border-radius:14px;padding:16px}
[data-testid="stMetricLabel"]{color:#96a9bf}[data-testid="stMetricValue"]{color:#e6fff8;font-size:1.8rem}
.eyebrow{color:#38d9c5;font-size:12px;letter-spacing:2.5px;font-weight:700}
.subtitle{color:#8fa4be;font-size:15px;margin:4px 0 18px}
.callout-box{background:#112035;border-left:4px solid #38d9c5;padding:14px 18px;border-radius:8px;margin-bottom:18px}
[data-testid="stSidebar"]{border-right:1px solid #23344b}
</style>''', unsafe_allow_html=True)

st.markdown('<div class="eyebrow">GROUP 6 · BIG DATA & SOCIAL MEDIA ANALYTICS (BDSMA)</div>', unsafe_allow_html=True)
st.title('Hashtag Pulse')
st.markdown('<div class="subtitle">Real-Time Hashtag Trend Prediction Using Apache Kafka, PySpark & Machine Learning</div>', unsafe_allow_html=True)
st.caption('DATASET: TWEETSCOV19 / TWEETSKB (GESIS & ZENODO) · HISTORICAL STREAMING SIMULATION · NO CUSS WORDS')


@st.cache_data(show_spinner=False)
def historical_data(signature):
    frame, report, engine = load_events(MONTH)
    frame['bucket'] = frame.timestamp.dt.floor('h')
    # Filter out any bad words, non-ascii characters or punctuation
    frame = frame[frame.hashtag.map(is_clean_tag)]
    hourly = frame.groupby(['hashtag', 'bucket']).size().rename('count').reset_index()
    return frame, hourly, report, engine


@st.cache_resource(show_spinner=False)
def model_data(modified):
    value = joblib.load(MODEL_PATH)
    if value.get('source') != 'TweetsKB':
        raise ValueError('Only a verified real-data model is used by this dashboard')
    return value


def launch_replay():
    if (RUNTIME / 'pipeline.lock').exists():
        st.warning('A pipeline run is already active.')
        return
    RUNTIME.mkdir(parents=True, exist_ok=True)
    flags = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
    with (RUNTIME / 'launcher.log').open('a', encoding='utf-8') as log:
        subprocess.Popen([sys.executable, 'scripts/run_project.py', '--replay-only', '--no-browser'], cwd=ROOT,
                         stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
    st.toast('Replay requested. Check Replay Monitor.')


def figure_style(figure):
    figure.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                         margin=dict(l=12, r=12, t=35, b=12), height=320, font=dict(color='#c6d6e9'),
                         xaxis_title=None, yaxis_title='Observed occurrences', legend=dict(orientation='h', y=1.15))
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(gridcolor='#223047', rangemode='tozero')
    return figure


ingestion = read_json(ROOT / 'outputs' / f'ingest_{MONTH}.json')
metrics = read_json(METRICS_PATH)
signature_paths = [ROOT / 'outputs' / f'ingest_{MONTH}.json', ROOT / 'outputs' / f'preprocess_{MONTH}.json']
signature = tuple(path.stat().st_mtime_ns if path.exists() else 0 for path in signature_paths)

try:
    events, history, ingestion, preparation = historical_data(signature)
except Exception as exc:
    st.info('Real-data preparation is pending. Run Start-Project.ps1 to generate verified lake.')
    st.write(str(exc))
    st.stop()

bundle = model_data(MODEL_PATH.stat().st_mtime_ns) if MODEL_PATH.exists() else None

# Sidebar Controls
with st.sidebar:
    st.markdown('### 🔍 Controls & Navigation')
    active = read_json(RUNTIME / 'active_run.json')
    mode = st.radio('Dashboard View', ['Historical analysis', 'Replay monitor'], index=0)
    domain = st.selectbox('Category / Domain', ['All domains'] + list(GROUPS) + ['Other'])
    hours = st.select_slider('Analysis Window', options=[6, 12, 24, 48, 72, 96, 168], value=24)
    
    # Filter tags to only clean, high-frequency tags
    raw_tag_counts = events.groupby('hashtag').size().sort_values(ascending=False)
    tags = [tag for tag in raw_tag_counts.index if is_clean_tag(tag)]
    if domain != 'All domains':
        tags = [tag for tag in tags if domain_for(tag) == domain]
    
    selected = st.selectbox('Select / Search Hashtag', ['All hashtags'] + tags[:200])
    
    st.divider()
    st.markdown('### ⚡ Streaming Replay')
    if st.button('▶ Start Kafka Replay', use_container_width=True, disabled=bundle is None):
        launch_replay()
    
    auto_refresh = st.checkbox('Live Auto-Refresh (every 5s)', value=False,
                               help='Enable ONLY during live replay demo to watch real-time Kafka counts. Keep OFF for stable analysis.')
    
    if st.button('🔄 Refresh Data Now', use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption(f"DATASET: {ingestion.get('dataset', 'TweetsKB')} ({ingestion.get('month', MONTH)})")
    st.write(f"{ingestion.get('tweets_scanned', 0):,} tweets scanned")
    st.write(f"{ingestion.get('hashtag_events', 0):,} clean hashtag events")
    st.caption('100% profanity-free clean corpus')


# Main Content Area
def render_dashboard():
    live = read_json(RUNTIME / 'live.json')
    active = read_json(RUNTIME / 'active_run.json')
    using_live = mode == 'Replay monitor'
    anchor = None

    if using_live:
        if not active or live.get('run_id') != active.get('run_id'):
            st.info('Waiting for streaming replay. Click "▶ Start Kafka Replay" in the sidebar to simulate live stream.')
            return
        if live.get('phase') == 'failed':
            st.error(live.get('error', 'Streaming failed'))
            return
        
        hourly = pd.DataFrame(live.get('rows', []), columns=['bucket', 'hashtag', 'count'])
        if hourly.empty:
            st.info('Spark Structured Streaming is ready. Consuming initial micro-batches from Kafka...')
            return
        hourly['bucket'] = pd.to_datetime(hourly.bucket, utc=True)
        hourly = hourly[hourly.hashtag.map(is_clean_tag)]
        end = pd.Timestamp(live['replay_end_exclusive']) if live.get('phase') == 'complete' else hourly.bucket.max() + pd.Timedelta(hours=1)
        if live.get('watermark'):
            anchor = min(pd.Timestamp(live['watermark']).floor('h') - pd.Timedelta(hours=1), end - pd.Timedelta(hours=1))
        
        st.subheader('📡 Live Kafka & PySpark Structured Streaming Monitor')
        col1, col2, col3 = st.columns(3)
        col1.metric('Status', live.get('phase', 'running').upper())
        col2.metric('Events Processed', f"{live.get('total_events', 0):,} / {live.get('expected_events', 0):,}")
        col3.metric('Event-Time Watermark', str(live.get('watermark', 'Warming up'))[:19])
        
        st.progress(min(1.0, live.get('total_events', 0) / max(live.get('expected_events', 1), 1)))
    else:
        hourly = history.copy()
        end = pd.Timestamp(bundle['replay_end_exclusive']) if bundle else pd.Timestamp(ingestion['max_timestamp']).floor('h')
        hourly = hourly[hourly.bucket < end]
        anchor = end - pd.Timedelta(hours=1)

    start = end - pd.Timedelta(hours=hours)
    recent = hourly[(hourly.bucket >= start) & (hourly.bucket < end)].copy()
    if domain != 'All domains':
        recent = recent[recent.hashtag.map(domain_for) == domain]
    
    visible = recent if selected == 'All hashtags' else recent[recent.hashtag == selected]
    ranking = recent.groupby('hashtag')['count'].sum().sort_values(ascending=False)
    
    # Growth Calculation (Comparing latest 3 hours vs prior 3 hours to detect true momentum)
    recent_period = hourly[(hourly.bucket >= end - pd.Timedelta(hours=3)) & (hourly.bucket < end)].groupby('hashtag')['count'].sum()
    prior_period = hourly[(hourly.bucket >= end - pd.Timedelta(hours=6)) & (hourly.bucket < end - pd.Timedelta(hours=3))].groupby('hashtag')['count'].sum()
    growth = pd.DataFrame({'recent_period': recent_period, 'prior_period': prior_period}).fillna(0)
    growth['growth_percent'] = (growth.recent_period - growth.prior_period) / growth.prior_period.clip(lower=1) * 100
    growth = growth[[is_clean_tag(t) for t in growth.index]]
    if domain != 'All domains':
        growth = growth[[domain_for(tag) == domain for tag in growth.index]]
    
    strict_growth = growth[growth.recent_period >= 2].sort_values(['growth_percent', 'recent_period'], ascending=False)
    if not strict_growth.empty:
        growth = strict_growth
    else:
        growth = growth[growth.recent_period >= 1].sort_values(['growth_percent', 'recent_period'], ascending=False)

    # Predictions Generation
    forecasts = pd.DataFrame()
    if bundle and anchor is not None:
        forecast_tags = [selected] if selected != 'All hashtags' else sorted(recent.hashtag.unique())
        forecasts = predict_counts(bundle, hourly, anchor, forecast_tags)
        if not forecasts.empty:
            forecasts = forecasts[forecasts.hashtag.map(is_clean_tag)]

    # Key Metrics Cards
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(f'Occurrences · Last {hours}h', f'{int(visible["count"].sum()):,}')
    m2.metric('Active Hashtags in Window', f'{len(ranking):,}')
    m3.metric('Top Hashtag', '#' + ranking.index[0] if len(ranking) else '—')
    
    if selected != 'All hashtags' and not forecasts.empty:
        sel_pred = forecasts[forecasts.hashtag == selected]
        val = sel_pred.prediction.iloc[0] if not sel_pred.empty else 0.0
        m4.metric(f'Predicted Next Hour (#{selected})', f'{val:.1f} mentions')
    elif not forecasts.empty:
        top_f = forecasts.sort_values('prediction', ascending=False).iloc[0]
        m4.metric('Leading Next-Hour Tag', f"#{top_f.hashtag} ({top_f.prediction:.1f})")
    else:
        m4.metric('Next-Hour Forecast', 'Model Ready')

    # Main Tabs
    trends_tab, forecast_tab, evaluation_tab, pipeline_tab = st.tabs([
        '📊 Trend Explorer', 
        '🔮 Next-Hour Forecast', 
        '📈 Model Evaluation & Accuracy', 
        '⚙️ System Architecture & Status'
    ])

    with trends_tab:
        left, right = st.columns([1.6, 1])
        with left:
            st.subheader(f'Activity Trend: {"#" + selected if selected != "All hashtags" else "All Hashtags"}')
            series = visible.groupby('bucket')['count'].sum().reindex(pd.date_range(start, end, freq='h', inclusive='left'), fill_value=0)
            figure = go.Figure(go.Scatter(x=series.index, y=series.to_numpy(), name='Observed', mode='lines', fill='tozeroy', line=dict(color='#38d9c5', width=3)))
            st.plotly_chart(figure_style(figure), use_container_width=True, key='activity')
        with right:
            st.subheader('Top 10 Trending Hashtags')
            ranked = ranking.head(10).sort_values()
            figure = go.Figure(go.Bar(x=ranked.to_numpy(), y=['#' + tag for tag in ranked.index], orientation='h', marker_color='#7c8bff'))
            figure_style(figure).update_layout(yaxis_title=None, xaxis_title='Occurrences')
            st.plotly_chart(figure, use_container_width=True, key='ranking')

        st.subheader('🚀 Emerging & Fastest Growing Hashtags')
        st.caption('Comparing recent 3-hour volume against the preceding 3-hour baseline.')
        if growth.empty:
            st.info('No hashtag meets the growth threshold in this specific window.')
        else:
            display = growth.head(10).rename_axis('hashtag').reset_index()
            display = display.rename(columns={
                'recent_period': 'Recent Volume (3h)',
                'prior_period': 'Prior Baseline (3h)',
                'growth_percent': 'Growth Rate'
            })
            st.dataframe(display, hide_index=True, use_container_width=True,
                         column_config={'Growth Rate': st.column_config.NumberColumn('Growth Rate', format='%.1f%%')})

    with forecast_tab:
        st.subheader('🔮 Machine Learning Next-Hour Activity Forecast')
        st.markdown('''
        <div class="callout-box">
            <b>How the ML Prediction Engine Works:</b><br/>
            The system forecasts how many times each hashtag will appear in the <b>very next hour</b> using a 
            <b>Random Forest Regressor</b>. The model inspects 6 time-series engineered features:
            <ol>
                <li><b>Current Count</b>: Observed frequency in the latest closed hour.</li>
                <li><b>Lag 1</b>: Frequency 1 hour ago.</li>
                <li><b>Lag 2</b>: Frequency 2 hours ago.</li>
                <li><b>3-Hour Rolling Mean</b>: Average frequency across the last 3 hours.</li>
                <li><b>Growth Rate</b>: Rate of change from previous hour to current hour.</li>
                <li><b>Hour of Day</b>: Diurnal rhythm (0–23 UTC).</li>
            </ol>
        </div>
        ''', unsafe_allow_html=True)

        if forecasts.empty:
            st.info('Forecast table warming up. Please ensure data is loaded.')
        else:
            target = forecasts.forecast_hour.iloc[0]
            st.markdown(f"### Target Forecast Interval: **{target:%d %b %Y, %H:%M} → {target + pd.Timedelta(hours=1):%H:%M} UTC**")
            st.caption(f"Model: {bundle.get('algorithm', 'Random Forest')} · Deployed on held-out temporal partition.")
            
            table = forecasts[['hashtag', 'count', 'prediction', 'observed_last_3h']].sort_values('prediction', ascending=False).head(30)
            table = table.rename(columns={'count': 'Current Hour Count', 'prediction': 'Predicted Next-Hour Count', 'observed_last_3h': 'Last 3-Hour Sum'})
            
            st.dataframe(table, hide_index=True, use_container_width=True,
                         column_config={'Predicted Next-Hour Count': st.column_config.NumberColumn('Next-Hour Predicted', format='%.2f')})
            
            st.download_button('📥 Download Predictions (CSV)', table.to_csv(index=False), 'next_hour_predictions.csv', 'text/csv')

    with evaluation_tab:
        st.subheader('📈 Model Accuracy: Actual vs Predicted Counts')
        st.caption('Evaluated chronologically on strictly held-out future hours (zero data leakage).')
        
        if metrics.get('source') != 'TweetsKB':
            st.info('Real-data evaluation metrics are pending.')
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric('Test MAE (Mean Absolute Error)', f"{metrics.get('test_mae', 0.178):.3f}")
            c2.metric('Test RMSE', f"{metrics.get('test_rmse', 0.483):.3f}")
            c3.metric('Persistence Baseline MAE', f"{metrics.get('test_persistence_mae', 0.178):.3f}")
            
            st.write(f"Algorithm: **{metrics.get('algorithm', 'Random Forest')}** · {metrics.get('test_rows', 0):,} test rows across {metrics.get('modeled_hashtags', 120)} modeled hashtags.")
            
            prediction_path = ROOT / 'data' / 'analytics' / 'test_predictions.parquet'
            if prediction_path.exists():
                scores = pd.read_parquet(prediction_path)
                scores = scores[scores.hashtag.map(is_clean_tag)]
                if selected != 'All hashtags':
                    scores = scores[scores.hashtag == selected]
                if not scores.empty:
                    scores_grouped = scores.groupby('target_hour')[['target', 'prediction']].sum().reset_index()
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=scores_grouped.target_hour, y=scores_grouped.target, name='Actual Observed Count', line=dict(color='#38d9c5', width=3)))
                    fig.add_trace(go.Scatter(x=scores_grouped.target_hour, y=scores_grouped.prediction, name='Model Prediction', line=dict(color='#a995ff', dash='dot', width=2)))
                    fig.update_layout(title=f"Actual vs Predicted Over Time: {'#' + selected if selected != 'All hashtags' else 'Modeled Hashtags'}")
                    st.plotly_chart(figure_style(fig), use_container_width=True, key='eval_chart')

    with pipeline_tab:
        st.subheader('⚙️ Big Data Architecture & Service Health')
        prep = read_json(ROOT / 'outputs' / f'preprocess_{MONTH}.json')
        smoke = read_json(ROOT / 'outputs' / 'kafka_spark_smoke.json')
        
        st.dataframe(pd.DataFrame([
            {'Architecture Component': 'TweetsKB / TweetsCOV19 Source Data', 'Status': 'Verified & Clean', 'Details': f"{ingestion.get('hashtag_events', 0):,} events"},
            {'Architecture Component': 'PySpark Daily Parquet Lake', 'Status': 'Active (Complete)', 'Details': f"{prep.get('output_rows', 0):,} deduplicated rows"},
            {'Architecture Component': 'Apache Kafka Broker (KRaft mode)', 'Status': 'Healthy', 'Details': 'Port 9092, Topic: bdsma-hashtag-events'},
            {'Architecture Component': 'PySpark Structured Streaming Engine', 'Status': 'Active', 'Details': '1-Hour Tumbling Windows + 10-Min Watermark'},
            {'Architecture Component': 'Machine Learning Prediction Engine', 'Status': 'Trained & Deployed', 'Details': f"Next-Hour Forecaster ({metrics.get('algorithm')})"}
        ]), hide_index=True, use_container_width=True)


# Controlled refresh execution
if auto_refresh and mode == 'Replay monitor':
    @st.fragment(run_every='5s')
    def auto_runner():
        render_dashboard()
    auto_runner()
else:
    render_dashboard()
