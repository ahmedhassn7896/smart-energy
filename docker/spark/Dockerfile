FROM apache/spark:4.0.1-java21-scala

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/ivy/cache /opt/ivy/local \
    && chmod -R 777 /opt/ivy

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_SUBMIT_OPTS="-Dspark.jars.ivy=/opt/ivy -Divy.home=/opt/ivy"

USER spark
