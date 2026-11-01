FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root
# A separate target directory avoids changing Ubuntu's system Python packages.
RUN python3 -m pip install --no-cache-dir --target /opt/bdsma-python kafka-python==2.3.3

ENV PYTHONPATH=/opt/bdsma-python:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
WORKDIR /workspace
USER spark
