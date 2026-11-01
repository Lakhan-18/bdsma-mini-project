"""One command to prepare real data, train, launch Streamlit and replay Kafka."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
import webbrowser

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.project import MONTH, RUNTIME, read_json, write_json


def stage(name, **details):
    print(f'[{name}]', flush=True)
    write_json(RUNTIME / 'pipeline.json', {'phase': name, 'updated_at': datetime.now(timezone.utc).isoformat(), **details})


def command(arguments, timeout=600):
    result = subprocess.run(arguments, cwd=ROOT, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'Command failed ({result.returncode}): {" ".join(arguments)}. See terminal output.')


def health(port):
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f'http://127.0.0.1:{port}/_stcore/health', timeout=2) as response:
            return response.status == 200
    except Exception:
        return False


def start_dashboard(open_browser=True):
    previous = read_json(RUNTIME / 'dashboard.json')
    if previous.get('project') == str(ROOT) and health(previous.get('port', 8501)):
        port = previous['port']
    else:
        port = next((number for number in range(8501, 8511) if not health(number)), None)
        if port is None:
            raise RuntimeError('Dashboard ports 8501-8510 are occupied')
        RUNTIME.mkdir(parents=True, exist_ok=True)
        log = (RUNTIME / 'dashboard.log').open('a', encoding='utf-8')
        flags = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        process = subprocess.Popen([sys.executable, '-m', 'streamlit', 'run', 'dashboard/app.py',
                                    '--server.address', '127.0.0.1', '--server.port', str(port),
                                    '--server.headless', 'true', '--browser.gatherUsageStats', 'false'],
                                   cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
        log.close()
        write_json(RUNTIME / 'dashboard.json', {'pid': process.pid, 'port': port, 'project': str(ROOT)})
        deadline = time.monotonic() + 90
        while not health(port):
            if process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('Streamlit did not become ready; see outputs/runtime/dashboard.log')
            time.sleep(1)
    url = f'http://localhost:{port}'
    print(f'DASHBOARD: {url}', flush=True)
    if open_browser:
        webbrowser.open(url)
    return url


def prepare_real_data():
    extraction = ROOT / 'data' / 'extracted' / MONTH / 'events.parquet'
    report = read_json(ROOT / 'outputs' / f'ingest_{MONTH}.json')
    if not extraction.exists() or report.get('status') != 'complete':
        partial = extraction.with_name(extraction.name + '.partial')
        if partial.exists():
            raise RuntimeError('An incomplete extraction exists. Preserve/check it before starting another extraction; no files were deleted.')
        stage('extracting_real_data')
        command([sys.executable, '-m', 'src.ingest', '--month', MONTH, '--cache-small-source'], timeout=1800)
    preprocessing = read_json(ROOT / 'outputs' / f'preprocess_{MONTH}.json')
    if preprocessing.get('status') != 'complete' or not (ROOT / 'data' / 'lake' / MONTH).is_dir():
        stage('preprocessing_with_spark')
        command(['docker', 'compose', 'run', '--rm', 'preprocess'], timeout=900)


def run_replay(speed):
    from src.replay import prepare_run, replay_events
    run, events = prepare_run(speed)
    stage('starting_stream', run_id=run['run_id'])
    command(['docker', 'compose', 'up', '-d', '--force-recreate', 'streamer'], timeout=300)
    deadline = time.monotonic() + 180
    while True:
        live = read_json(RUNTIME / 'live.json')
        if live.get('run_id') == run['run_id']:
            if live.get('phase') == 'failed':
                raise RuntimeError(live.get('error', 'Spark streaming failed'))
            if live.get('phase') == 'running':
                break
        if time.monotonic() > deadline:
            raise TimeoutError('Spark stream startup timed out. Run docker compose logs --tail 60 streamer')
        time.sleep(2)
    stage('replaying_real_data', run_id=run['run_id'])
    replay_events(run, events)
    stage('finalizing_windows', run_id=run['run_id'])
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        live = read_json(RUNTIME / 'live.json')
        if live.get('run_id') == run['run_id']:
            if live.get('phase') == 'failed':
                raise RuntimeError(live.get('error', 'Stream failed'))
            if live.get('phase') == 'complete':
                stage('complete', run_id=run['run_id'], events_processed=live['total_events'])
                print('FULL REAL-DATA REPLAY COMPLETE: all events counted and windows finalized.', flush=True)
                return
        time.sleep(2)
    raise TimeoutError('Watermark/final count did not complete. Inspect outputs/runtime/live.json and Docker streamer logs.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dashboard-only', action='store_true')
    parser.add_argument('--replay-only', action='store_true')
    parser.add_argument('--no-browser', action='store_true')
    parser.add_argument('--speed', type=float, default=14400, help='Source seconds per wall-clock second')
    args = parser.parse_args()
    RUNTIME.mkdir(parents=True, exist_ok=True)
    if args.dashboard_only:
        start_dashboard(not args.no_browser)
        return
    lock = RUNTIME / 'pipeline.lock'
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise SystemExit('A project launch is already active. Check the terminal and outputs/runtime/pipeline.json.')
    os.close(descriptor)
    try:
        if not args.replay_only:
            stage('checking_kafka')
            command(['docker', 'compose', 'up', '-d', '--wait', '--wait-timeout', '120', 'kafka'], timeout=180)
            prepare_real_data()
            stage('training_real_model')
            from src.analytics import train_real
            print(train_real(), flush=True)
            start_dashboard(not args.no_browser)
        run_replay(args.speed)
    except Exception as exc:
        stage('failed', error=f'{type(exc).__name__}: {exc}')
        print(f'PROJECT ERROR: {exc}', flush=True)
        raise SystemExit(1)
    finally:
        lock.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
