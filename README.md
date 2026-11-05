Data Lineage and Responsible AI Using OpenLineage

An end-to-end data lineage and responsible AI monitoring system integrating Apache Spark, Apache Kafka, Apache Flink, OpenLineage and Marquez.

1. Project Overview

Machine learning pipelines depend on multiple datasets, transformations, models and downstream applications. When a source dataset is found to be biased or unreliable, identifying every affected downstream component becomes difficult without proper data lineage.

This project addresses that problem by implementing an integrated batch and streaming data processing system that captures data dependencies and supports downstream impact analysis.

The system demonstrates how OpenLineage and Marquez can be used to trace datasets, processing jobs, machine learning models and dashboard outputs.

Key Objectives

* Implement distributed batch processing using Apache Spark.
* Implement real-time event processing using Apache Kafka and Apache Flink.
* Capture dataset and job dependencies using OpenLineage.
* Visualise lineage metadata using Marquez.
* Identify downstream models, datasets and dashboards affected by a source dataset.
* Evaluate group-level disparities using responsible AI fairness metrics.
* Provide a reproducible environment using Docker Compose.

2. System Architecture

The project consists of two processing pipelines that share the same lineage backend.

Architecture Diagram

flowchart TB
    subgraph Batch["Batch Processing — Apache Spark"]
        A["Synthetic Loan Dataset"] --> B["Data Validation"]
        B --> C["Processed Dataset"]
        C --> D["Model Training"]
        D --> E["Loan Approval Model"]
        E --> F["Prediction Generation"]
        C --> F
        F --> G["Fairness Dashboard"]
    end
    subgraph Streaming["Streaming Processing — Kafka and Flink"]
        H["Synthetic Event Producer"] --> I["Kafka Topic"]
        I --> J["Flink Streaming Job"]
        J --> K["Event Validation"]
        K --> L["Windowed Fairness Metrics"]
    end
    subgraph Lineage["Lineage and Impact Analysis"]
        M["OpenLineage Events"] --> N["Marquez Backend"]
        N --> O["Lineage Graph"]
        N --> P["Downstream Impact Analysis"]
    end
    B -.-> M
    D -.-> M
    F -.-> M
    G -.-> M
    J -.-> M

Technology Stack

Technology	Purpose
Python 3.11	Data generation and supporting scripts
Apache Spark 3.5.6	Distributed batch processing and model training
Apache Kafka 3.9.1	Streaming event ingestion
Apache Flink 1.20.5	Real-time event processing
OpenLineage	Standardised lineage event collection
Marquez 0.50.0	Lineage metadata storage and visualisation
PostgreSQL	Marquez metadata database
Docker Compose	Container orchestration
scikit-learn / Spark ML	Machine learning
NumPy	Fairness metric calculations
pytest	Automated testing

3. Batch Processing Pipeline

The batch pipeline processes synthetic loan application data using Apache Spark.

Processing Stages

1. Data Generation: Generate 5,000 synthetic loan application records with group-dependent approval patterns.
2. Data Validation: Validate records and separate valid and rejected data.
3. Data Processing: Prepare the processed dataset for model training.
4. Model Training: Train a logistic regression loan approval model.
5. Prediction Generation: Generate predictions using the trained model.
6. Fairness Evaluation: Calculate group-level approval rates and fairness indicators.
7. Lineage Capture: Emit OpenLineage events to Marquez to record dataset and job dependencies.

Batch Lineage

The principal lineage relationships are:

synthetic_loan_applications
          |
          v
loan_data_validation
          |
          v
processed_loan_applications
          |
          v
loan_model_training
          |
          v
loan_approval_model
          |
          v
loan_prediction_generation
          |
          v
loan_predictions
          |
          v
loan_fairness_dashboard
          |
          v
approval_dashboard

The prediction-generation job also consumes the processed dataset.

4. Streaming Processing Pipeline

The streaming pipeline uses Apache Kafka and Apache Flink to process synthetic loan application events.

Processing Stages

1. Generate synthetic loan application events.
2. Publish events to the Kafka topic loan_application_events.
3. Consume events using a Flink streaming job.
4. Validate required fields and event values.
5. Aggregate events using one-minute tumbling processing-time windows.
6. Calculate group-level approval and fairness metrics.
7. Emit OpenLineage events to Marquez.

Streaming Fairness Metrics

The streaming pipeline calculates:

* Approval rate for each synthetic group.
* True positive rate for each synthetic group.
* Demographic Parity Difference.
* Demographic Parity Ratio.
* Equal Opportunity Difference.

The metrics are calculated over streaming windows and should be interpreted with consideration for the number of observations in each window.

Streaming Lineage

Kafka Topic
    |
    v
Loan Risk Streaming Pipeline
    |
    v
Streaming Fairness Metrics

The Flink job records its execution using OpenLineage, allowing the streaming input and output datasets to be explored in Marquez.

5. Responsible AI Evaluation

The project uses synthetic loan application data with intentionally introduced group-dependent approval patterns.

The fairness evaluation compares outcomes between two synthetic groups, A and B.

The following indicators are measured:

Metric	Description
Approval Rate	Proportion of applications approved within a group
Demographic Parity Difference	Absolute difference in predicted approval rates between groups
Demographic Parity Ratio	Ratio of predicted approval rates between groups
True Positive Rate	Proportion of eligible applicants correctly approved
Equal Opportunity Difference	Difference in true positive rates between groups

These metrics provide complementary information about model behaviour.

A fairness ratio close to 1 does not independently establish that a model is fair. The metrics must be interpreted together with group sample sizes, data quality and the application context.

6. Experimental Results

The batch pipeline processed 5,000 synthetic loan applications.

Metric	Result
Total input records	5,000
Valid records	5,000
Rejected records	0
Training records	4,042
Test records	958
Test accuracy	89.77%
Actual approval-rate gap	7.79 percentage points
Predicted approval-rate gap	1.29 percentage points
Demographic parity ratio	0.9811

The experiment demonstrates why model accuracy alone is insufficient for evaluating responsible AI systems.

The streaming pipeline independently calculates fairness metrics over incoming events, allowing changes in group-level outcomes to be observed over time.

All experimental data is synthetic and does not represent real loan applicants.

7. Downstream Impact Analysis

A key use case is identifying the consequences of discovering bias in an upstream dataset.

The impact analysis queries lineage metadata from the Marquez API and traverses downstream dependencies.

For the source dataset:

synthetic_loan_applications

The analysis identifies:

Affected datasets

* processed_loan_applications
* loan_approval_model
* loan_predictions
* approval_dashboard

Affected model

* loan_approval_model

Affected dashboard

* approval_dashboard

This allows data engineers and responsible AI teams to identify which downstream assets require investigation when an upstream dataset is flagged.

The analysis identifies dependencies; it does not automatically retrain models or prove that a deployed model is currently serving predictions.

8. Repository Structure

data-lineage-openlineage/
│
├── data/
│   ├── generate_synthetic_data.py
│   └── synthetic_loan_applications.csv
│
├── spark/
│   └── batch_pipeline.py
│
├── kafka/
│   └── produce_events.py
│
├── flink/
│   ├── Dockerfile
│   ├── streaming_job.py
│   ├── complete_streaming_lineage.py
│   └── connectors/
│
├── lineage/
│   ├── lineage_client.py
│   └── impact_analysis.py
│
├── responsible_ai/
│   └── fairness_metrics.py
│
├── reports/
│   ├── batch_metrics.json
│   ├── impact_analysis.json
│   └── model_card.md
│
├── notebooks/
│
├── tests/
│
├── docs/
│
├── docker-compose.yml
├── docker-compose.streaming.yml
├── README.md
└── .gitignore

9. Setup and Execution

Prerequisites

* Docker Desktop
* Python 3.11
* Git
* At least 8 GB of available memory recommended for running the containerised stack.

Step 1: Clone the Repository

git clone https://github.com/nishithasathya/data-lineage-openlineage.git
cd data-lineage-openlineage

Step 2: Start Infrastructure

docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d

Check the running containers:

docker compose -f docker-compose.yml -f docker-compose.streaming.yml ps

Step 3: Access Marquez

Open:

* Marquez UI: http://localhost:3001
* Marquez API: http://localhost:5002

Use the loan-risk-demo namespace to inspect the project lineage.

Step 4: Run the Batch Pipeline

docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --packages io.openlineage:openlineage-spark_2.12:1.54.0 \
  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
  --conf spark.openlineage.transport.type=http \
  --conf spark.openlineage.transport.url=http://marquez-api:5000 \
  --conf spark.openlineage.transport.endpoint=/api/v1/lineage \
  --conf spark.openlineage.namespace=loan-risk-demo \
  /opt/project/spark/batch_pipeline.py

Step 5: Run the Streaming Pipeline

Create the Kafka topic:

docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic loan_application_events \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1

Install the host-side Kafka producer dependency:

python3 -m pip install kafka-python

Start the Flink job first:

docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec jobmanager \
  /opt/flink/bin/flink run -d -py /opt/project/flink/streaming_job.py

In a separate terminal, start the Kafka producer:

python3 kafka/produce_events.py

The producer publishes synthetic events continuously. Stop it using Ctrl+C when sufficient events have been generated.

Step 6: Complete Streaming Lineage

After stopping the producer, cancel the Flink job using its actual job ID:

docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec jobmanager \
  /opt/flink/bin/flink list

Then:

docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec jobmanager \
  /opt/flink/bin/flink cancel <JOB_ID>

Send the OpenLineage completion event:

docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec jobmanager \
  python /opt/project/flink/complete_streaming_lineage.py

The completion script uses the saved run ID to emit a COMPLETE event to Marquez.

Step 7: Run Downstream Impact Analysis

Install the required Python dependencies if they are not already installed:

python3 -m pip install requests

Run:

python3 lineage/impact_analysis.py \
  --dataset synthetic_loan_applications \
  --output reports/impact_analysis.json

Step 8: Run Automated Tests

python3 -m pip install pytest numpy

Run:

python3 -m pytest -v

10. Validation and Testing

The project includes automated tests for:

* Fairness metric calculations.
* Group-level approval and true positive rates.
* Input validation and invalid values.
* Downstream lineage graph traversal.
* Duplicate dependency handling.
* Disconnected datasets.
* Lineage-related project structure checks.

The automated test suite previously completed with 17 passing tests.

The Spark and Flink pipelines were also executed separately, with their lineage runs verified in Marquez.

11. Limitations

* The experiments use synthetic data and do not establish fairness in real lending environments.
* Fairness metrics depend on sample sizes and the definition of the comparison groups.
* Streaming fairness metrics are calculated over individual time windows and may fluctuate.
* The impact analysis identifies recorded lineage dependencies but does not automatically retrain affected models.
* The current project demonstrates lineage tracking and impact identification rather than a production deployment or automated governance platform.

12. Team Contributions

The project was developed collaboratively, with responsibilities divided across batch processing, streaming processing, lineage integration and responsible AI evaluation.

Batch Processing and Lineage

* Synthetic dataset generation.
* Apache Spark batch processing.
* Batch OpenLineage instrumentation.
* Marquez integration.
* Downstream impact analysis.
* Fairness evaluation and automated tests.

Streaming Processing

* Kafka synthetic event producer.
* Apache Flink streaming pipeline.
* Streaming OpenLineage instrumentation.
* Windowed fairness metric calculations.
* Streaming validation and testing.

13. Conclusion

This project demonstrates how data lineage can support responsible AI and distributed data processing.

By combining Spark batch processing, Kafka event ingestion, Flink streaming, OpenLineage and Marquez, the system provides visibility into data dependencies across machine learning workflows.

When an upstream dataset is identified as potentially biased, downstream lineage can help identify affected datasets, models and dashboards that require further investigation.

The project demonstrates the importance of combining model performance evaluation with transparency, traceability and fairness monitoring.