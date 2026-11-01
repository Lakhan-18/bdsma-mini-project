"""Read-only checks with timeouts so unavailable runtimes cannot hang setup."""
import importlib.metadata
import shutil
import subprocess
import sys

print('Python:', sys.version)
missing = []
for package in ['pyspark', 'kafka-python', 'streamlit', 'pandas', 'pyarrow', 'scikit-learn']:
    try:
        print(package, importlib.metadata.version(package))
    except importlib.metadata.PackageNotFoundError:
        print(package, 'MISSING')
        missing.append(package)
for command in [['java', '-version'], ['docker', 'info', '--format', '{{.ServerVersion}}']]:
    if not shutil.which(command[0]):
        print(command[0], 'MISSING')
        missing.append(command[0])
        continue
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        print(command[0], result.returncode, result.stdout.strip(), result.stderr.strip())
        if result.returncode:
            missing.append(command[0])
        elif command[0] == 'java':
            import re
            match = re.search(r'version "(\d+)', result.stderr + result.stdout)
            if not match or int(match.group(1)) != 17:
                print('Java 17 is required by this project setup.')
                missing.append('Java 17')
    except subprocess.TimeoutExpired:
        print(command[0], 'TIMED OUT')
        missing.append(command[0])
if missing:
    print('NOT READY:', ', '.join(missing))
    raise SystemExit(1)
print('Base dependencies are ready. Kafka topic creation and Spark connector smoke tests are still required.')
