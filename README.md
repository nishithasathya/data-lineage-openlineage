
# Data Lineage and Responsible AI using OpenLineage

End-to-end data lineage and responsible AI monitoring using Apache Spark, Apache Flink, Kafka, OpenLineage, and Marquez.

## 1. Project Overview

This project demonstrates how data lineage can be used to trace the impact of biased datasets across downstream machine learning pipelines.

It includes:
- Batch data processing using Apache Spark.
- Real-time event processing using Apache Flink and Kafka.
- OpenLineage instrumentation for tracking data dependencies.
- Marquez for lineage visualization.
- Fairness evaluation using demographic parity and equal opportunity metrics.

## 2. Architecture

### Batch Pipeline
Synthetic Loan Data → Spark Processing → Model/Approval Metrics → OpenLineage → Marquez

### Streaming Pipeline
Kafka Producer → Kafka Topic → Flink Streaming Job → Fairness Metrics → OpenLineage → Marquez

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Python 3.11 | Event generation and processing |
| Apache Kafka | Streaming event ingestion |
| Apache Flink | Real-time processing |
| Apache Spark | Batch processing |
| OpenLineage | Data lineage events |
| Marquez | Lineage metadata and visualization |
| PostgreSQL | Marquez metadata storage |
| Docker Compose | Infrastructure management |

## 4. Streaming Component

The streaming component generates synthetic loan application events and publishes them to Kafka.

Each event contains:
- Application ID
- Event timestamp
- Age
- Annual income
- Credit score
- Loan amount
- Employment years
- Synthetic demographic group
- Ground-truth eligibility
- Predicted loan approval

### Controlled Bias Experiment

The producer generates an unbiased eligibility label and introduces a group-dependent adjustment to the approval score.

This allows comparison of actual eligibility and predicted approval outcomes across synthetic groups.

### Fairness Metrics

The streaming pipeline calculates:

- Approval rate by group
- Demographic parity difference
- Demographic parity ratio
- True positive rate by group
- Equal opportunity difference

Ground-truth eligibility is used as `y_true`, predicted approval as `y_pred`, and synthetic group as the protected-group proxy.

## 5. Setup

### Prerequisites
- Docker Desktop
- Python 3.11
- Git

### Start shared infrastructure

```bash
docker compose up -d
```

### Start Kafka and Flink

```bash
docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d kafka jobmanager taskmanager
```

### Activate Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install kafka-python numpy
```

## 6. Run the Streaming Pipeline

### Start the Kafka producer

```bash
python kafka/produce_events.py
```

The producer continuously publishes synthetic loan application events to:

`loan_application_events`

### Submit the Flink job

In another terminal:

```bash
docker exec loan-flink-jobmanager sh -c 'export PYTHONPATH=/opt/flink/lineage:$PYTHONPATH && /opt/flink/bin/flink run -py /opt/flink/streaming_job.py'
```

The Flink job consumes Kafka events and calculates streaming fairness metrics.

## 7. OpenLineage Run Completion

The streaming job automatically saves its OpenLineage run UUID to:

`/opt/flink/streaming_run_id.txt`

After stopping the Flink job, complete the corresponding Marquez run using:

```bash
docker exec loan-flink-jobmanager sh -c 'export PYTHONPATH=/opt/flink/lineage:$PYTHONPATH && /opt/pyflink-venv/bin/python /opt/flink/complete_streaming_lineage.py'
```

This sends a COMPLETE event using the saved run ID.

## 8. View Lineage

Marquez UI:

http://localhost:3001

Marquez API:

http://localhost:5002

Streaming job:
`loan-risk-demo.Loan Risk Streaming Pipeline`

Input dataset:
`loan-risk-demo.loan_application_events`

Output dataset:
`loan-risk-demo.streaming_fairness_metrics`

The lineage graph connects the Kafka input dataset to the Flink processing job and its fairness-metrics output dataset.

## 9. Responsible AI Evaluation

The controlled-bias experiment evaluates whether group-dependent approval decisions produce disparities between synthetic demographic groups.

The fairness metrics are calculated over streaming event windows.

Results should be interpreted using sufficiently large samples, rather than individual small windows.

## 10. Team Contributions

### Batch Processing
- Spark batch pipeline
- Batch OpenLineage integration
- Impact-analysis queries
- Shared Docker and Marquez infrastructure

### Streaming and Responsible AI
- Kafka synthetic loan event producer
- Flink streaming pipeline
- Streaming OpenLineage instrumentation
- Streaming fairness metrics
- Controlled-bias experiment
- Streaming evaluation and model card

## 11. Repository Structure

```text
data-lineage-openlineage/
├── data/
├── kafka/
│   └── produce_events.py
├── flink/
│   ├── Dockerfile
│   ├── streaming_job.py
│   ├── complete_streaming_lineage.py
│   └── connectors/
├── lineage/
│   └── lineage_client.py
├── responsible_ai/
│   └── fairness_metrics.py
├── spark/
├── reports/
├── docker-compose.yml
├── docker-compose.streaming.yml
└── README.md
```
