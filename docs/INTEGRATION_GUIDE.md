# Integration Guide

## Overview
This project integrates Apache Spark, Kafka, Flink, OpenLineage and Marquez to demonstrate data lineage tracking and Responsible AI using synthetic loan application data.

## Prerequisites
- Docker Desktop
- Python 3.11+
- Git

## Setup
Clone the repository:

    git clone https://github.com/nishithasathya/data-lineage-openlineage.git
    cd data-lineage-openlineage

Start all services:

    docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d

## Service URLs

| Service | URL |
|---|---|
| Marquez | http://localhost:3001 |
| Marquez API | http://localhost:5002 |
| Flink | http://localhost:8081 |

Namespace: `loan-risk-demo`

## Batch Pipeline

Run the Spark batch pipeline:

    docker compose exec spark /opt/spark/bin/spark-submit \
      --master 'local[*]' \
      --packages io.openlineage:openlineage-spark_2.12:1.54.0 \
      --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
      --conf spark.openlineage.transport.type=http \
      --conf spark.openlineage.transport.url=http://marquez-api:5000 \
      --conf spark.openlineage.transport.endpoint=/api/v1/lineage \
      --conf spark.openlineage.namespace=loan-risk-demo \
      /opt/project/spark/batch_pipeline.py

## Streaming Pipeline

Create the Kafka topic:

    docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic loan_application_events --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

Submit the Flink job:

    docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec jobmanager /opt/flink/bin/flink run -d -py /opt/project/flink/streaming_job.py

Install the producer dependency and start publishing events:

    python3 -m pip install kafka-python
    python3 kafka/produce_events.py

Start the Flink job before the producer so new events are consumed.

## Impact Analysis

Identify downstream assets:

    python3 lineage/impact_analysis.py --dataset synthetic_loan_applications --output reports/impact_analysis.json

The analysis identifies affected datasets, models, predictions and dashboards.

## Testing

Run the automated tests:

    python3 -m pip install pytest
    python3 -m pytest -v

Recorded result: **17 tests passed.**

## Results

- Batch model accuracy: 89.77%
- Actual approval-rate gap: 7.79 percentage points
- Predicted approval-rate gap: 1.29 percentage points
- Demographic parity ratio: 0.9811
- Equal opportunity difference: approximately 7.27 percentage points

## Lineage

Marquez visualizes the following batch dependencies:

`synthetic_loan_applications → processed_loan_applications → loan_approval_model → loan_predictions → approval_dashboard`

Streaming dependencies:

`loan_application_events → Loan Risk Streaming Pipeline → streaming_fairness_metrics`

## Limitations

The dataset is synthetic. Fairness metrics cover only selected dimensions, and lineage identifies affected assets without automatically retraining or disabling models.

## Conclusion

The project demonstrates how Big Data processing, lineage tracking and fairness evaluation can work together to improve transparency, traceability and responsible data governance.

## Stop Services

    docker compose -f docker-compose.yml -f docker-compose.streaming.yml down