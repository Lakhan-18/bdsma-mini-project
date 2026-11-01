import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
from src.spark_runtime import configure_spark_environment

TEST_ROOT = Path(__file__).resolve().parents[1] / 'outputs' / 'test_scratch'
TEST_ROOT.mkdir(parents=True, exist_ok=True)


class SparkRuntimeTests(unittest.TestCase):
    def test_distribution_layout_uses_spark_home(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            home = Path(directory).resolve()
            (home / 'jars').mkdir()
            python_package = home / 'python' / 'pyspark' / '__init__.py'
            with patch.dict(os.environ, {'SPARK_HOME': str(home)}):
                with patch('src.spark_runtime.importlib.util.find_spec', return_value=SimpleNamespace(origin=str(python_package))):
                    self.assertEqual(configure_spark_environment(), home)

    def test_pip_layout_takes_precedence_over_old_home(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            home = Path(directory).resolve()
            (home / 'jars').mkdir()
            with patch.dict(os.environ, {'SPARK_HOME': str(home / 'old-spark')}):
                with patch('src.spark_runtime.importlib.util.find_spec', return_value=SimpleNamespace(origin=str(home / '__init__.py'))):
                    self.assertEqual(configure_spark_environment(), home)


if __name__ == '__main__':
    unittest.main()
