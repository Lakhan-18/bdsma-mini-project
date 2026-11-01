"""Select this environment's Spark installation without Windows path guessing."""
import importlib.util
import os
from pathlib import Path
import sys


def configure_spark_environment():
    spec = importlib.util.find_spec('pyspark')
    if spec is None or spec.origin is None:
        raise RuntimeError('PySpark is missing from this Python environment. Run Setup.ps1.')
    candidates = [Path(spec.origin).resolve().parent]
    # Official Spark distributions put Python under SPARK_HOME/python/pyspark
    # while pip installations keep jars inside the Python package itself.
    if os.environ.get('SPARK_HOME'):
        candidates.append(Path(os.environ['SPARK_HOME']).resolve())
    spark_home = next((candidate for candidate in candidates
                       if (candidate / 'jars').is_dir()), None)
    if spark_home is None:
        raise RuntimeError('Could not locate Spark jars in the Python package or SPARK_HOME')
    os.environ['SPARK_HOME'] = str(spark_home)
    if os.name == 'nt':
        # Spark's find-spark-home.cmd expands PYTHON_RUNNER without quoting it.
        # Resolve python.exe through this environment's Scripts directory instead.
        os.environ['PATH'] = str(Path(sys.executable).parent) + os.pathsep + os.environ.get('PATH', '')
        python_runner = Path(sys.executable).name
    else:
        python_runner = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_runner
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_runner
    os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
    return spark_home
