# Hashtag Pulse — Real-Time Hashtag Trend Prediction

The project uses real TweetsKB metadata and historical replay. It does not use a live Twitter API. The laptop demonstration processes a bounded February 2013 source; it must not be described as having processed all 3.1 billion tweets.

## Start the complete project

Open Docker Desktop, then double-click **Start-Project.cmd**, or run the following from this folder in a normal Windows PowerShell terminal:

```powershell
.\Start-Project.ps1
```

The launcher checks Kafka, completes missing real-data preparation, runs PySpark preprocessing, trains and evaluates the model, opens Streamlit, and replays the held-out period through Kafka and PySpark Structured Streaming. It normally opens http://localhost:8501 and prints the exact URL if another port is needed. Run `Start-Project.ps1 -DashboardOnly` to explore existing results without starting a new replay.

If an incomplete extraction exists, the launcher preserves it and refuses to overwrite it. Check whether its original process is still running before recovery. If automatic approval review has blocked a command, that action must be resolved separately; the launcher is not a way to bypass the block.

## Actual architecture

1. `src/ingest.py`: bounded Turtle-block parsing with RDFLib; source checksum verification; skinny Parquet extraction. Only ID, UTC timestamp and hashtag are retained.
2. `scripts/preprocess_tweetskb.py`: PySpark validation, global tweet/hashtag deduplication and date-partitioned Parquet lake.
3. `src/analytics.py`: time-based train/validation/test features and next-hour count prediction. Random Forest is compared with persistence on validation; the chosen model is evaluated on later test hours.
4. `src/replay.py`: event-time-sorted real records sent to a one-partition Kafka topic using idempotent production. The held-out interval has three hours of warm-up history.
5. `scripts/stream_hashtags.py`: Kafka Structured Streaming, hourly windows and a ten-minute watermark. Absolute aggregate counts are upserted idempotently into SQLite on a Linux Docker volume.
6. `dashboard/app.py`: Streamlit explorer, domain/time/search controls, frequency/growth rankings, next-hour forecasts, real-data evaluation and replay health.

SQLite and checkpoints stay inside a Linux volume. The Windows dashboard reads atomic JSON exports, avoiding a shared SQLite database across operating systems. A unique run ID and a new checkpoint/state directory isolate replays. Explicit end-of-replay clock messages finalize windows and never contribute to hashtag totals. A run is complete only when counts match the expected number of events, the final offset is consumed and the watermark has passed the end of the replay interval.

## Forecast and evaluation interpretation

The chronological split is 60% training / 20% validation / 20% testing. Hashtag selection uses only the training period. Labels are separated by target-hour boundaries. The deployed model is fitted only to hours preceding the test period, so the replay does not train on its own future. Dashboard live forecasts use finalized windows and the same feature function definitions as training: current count, two lags, three-hour mean, growth and hour of day.

Forecasts are point estimates of observed dataset occurrences. They are not probabilities, confidence intervals or predictions of all Twitter traffic. Sparse data can make overall MAE look small; evaluation also reports MAE on nonzero target rows and a persistence baseline. Domain filters are keyword rules, not an NLP classifier, and do not restrict ingestion to four topics.

## Dashboard demonstration

- Historical analysis: select a domain, time window and hashtag; inspect activity, rankings and the next-hour estimate.
- Replay monitor: watch processed-event progress, event-time activity and finalized-window forecasts update every two seconds.
- Model evaluation: compare unseen actual counts with predictions and inspect MAE/RMSE/baseline.
- Pipeline status: inspect persisted source, preprocessing and connector checks. These are labelled as latest verified results rather than assumed live engine health.
- CSV downloads: export growth rankings and forecasts.

## Storage and runtime limits

The initial source archive is approximately 9 MB. Extraction caps compressed input at 20 MiB, decompressed input at 512 MiB, one tweet block at 1 MiB and output at 64 MiB. The host model/dashboard limits a demo to 500,000 events and features to two million rows. The streaming snapshot is limited to 100,000 hourly rows over the latest 96 hours. Larger data needs bounded intervals and new budgets rather than blindly downloading the full collection. The Spark image is downloaded once and reused; it is runtime software, not the dataset.

## Useful paths

| Output | Purpose |
|---|---|
| `outputs/ingest_2013-02.json` | Real source integrity and extraction result |
| `data/lake/2013-02` | PySpark-prepared Parquet |
| `models/real/next_hour.joblib` | Deployed model and temporal provenance |
| `outputs/real_metrics.json` | Real-data evaluation |
| `data/analytics/test_predictions.parquet` | Held-out actual/predicted counts |
| `outputs/runtime/pipeline.json` | Orchestration stage or error |
| `outputs/runtime/live.json` | Streaming counts and watermark |
| `outputs/runtime/replay.json` | Kafka producer progress |
| `outputs/runtime/dashboard.log` | Streamlit server log |

Run tests with `.\.venv\Scripts\python.exe -m unittest discover -s tests -v`. Unit fixtures are synthetic and are never used as real-data dashboard results. The original synthetic prototype is archived in `dashboard/legacy_app.py` and is not launched by this project.

## Stop background components

`docker compose stop streamer kafka` stops only this project's containers and keeps their named volumes. To stop Streamlit, read its PID from `outputs/runtime/dashboard.json` and stop that specific process after verifying it belongs to this project's Streamlit command. No launcher deletes Docker volumes or other projects.
