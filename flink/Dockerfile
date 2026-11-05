
FROM flink:1.20.5-scala_2.12

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-venv python3-pip && \
    python3 -m venv /opt/pyflink-venv && \
    /opt/pyflink-venv/bin/pip install --no-cache-dir apache-flink==1.20.5 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/pyflink-venv/bin:${PATH}"
ENV PYFLINK_CLIENT_EXECUTABLE="/opt/pyflink-venv/bin/python"
COPY connectors/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
USER flink
