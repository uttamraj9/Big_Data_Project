FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

RUN pip install --no-cache-dir pyspark==3.5.3 numpy

WORKDIR /app

COPY train_rf_from_hive.py .

CMD ["spark-submit", "--master", "local[*]", "train_rf_from_hive.py"]
