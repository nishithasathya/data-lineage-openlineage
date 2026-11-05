# data-lineage-openlineage
End-to-end data lineage and responsible AI using OpenLineage, Marquez, Spark and Flink

Spark Batch Processing and Downstream Impact Analysis

This module implements the batch processing and data lineage components of the Data Lineage with OpenLineage project.

The objective is to identify downstream datasets, machine learning models and dashboards that may be affected when a source dataset is found to contain bias.

My Contributions

* Generated a synthetic loan application dataset with intentional group-dependent approval bias.
* Implemented data validation and batch processing using Apache Spark.
* Trained a logistic regression loan approval model using Spark ML.
* Integrated OpenLineage to capture pipeline dependencies.
* Connected lineage metadata to Marquez for visualization.
* Implemented downstream impact analysis using graph traversal.
* Developed fairness metrics to evaluate group-level prediction differences.
* Created automated tests for fairness calculations and lineage analysis.

Architecture

flowchart TD
    A[Synthetic Loan Dataset] --> B[Spark Data Validation]
    B --> C[Processed Dataset]
    C --> D[Model Training]
    D --> E[Loan Approval Model]
    C --> F[Prediction Generation]
    E --> F
    F --> G[Loan Predictions]
    G --> H[Fairness Evaluation]
    H --> I[Approval Dashboard]
    B -.-> J[OpenLineage]
    D -.-> J
    F -.-> J
    H -.-> J
    J --> K[Marquez]

Technologies Used

* Python
* Apache Spark 3.5.6
* Spark ML
* OpenLineage
* Marquez
* PostgreSQL
* Docker Compose
* NumPy
* Pytest

Dataset

A synthetic dataset containing 5,000 loan applications.

Features include:

* Age
* Annual income
* Credit score
* Loan amount
* Employment years
* Synthetic group
* Approval outcome

No real personal or financial data is used.

Lineage Tracking

The batch pipeline records dependencies for four logical jobs:

1. loan_data_validation
2. loan_model_training
3. loan_prediction_generation
4. loan_fairness_dashboard

Marquez provides a visual representation of the recorded lineage.

Downstream Impact Analysis

The impact analysis module is implemented in:

lineage/impact_analysis.py

It traverses the lineage graph to identify downstream datasets, models, dashboards and jobs affected by a flagged dataset.

Run:

python3 lineage/impact_analysis.py --dataset synthetic_loan_applications

Responsible AI

The fairness module evaluates:

* Demographic Parity Difference
* Demographic Parity Ratio
* Equal Opportunity Difference

These metrics help identify differences in model outcomes across synthetic groups.

Testing

Run the automated tests:

python3 -m pytest -v

Current verified result: 17 tests passed.

Running the Batch Pipeline

Start the Docker services:

docker compose up -d

Run the Spark pipeline:

docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --packages io.openlineage:openlineage-spark_2.12:1.54.0 \
  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
  --conf spark.openlineage.transport.type=http \
  --conf spark.openlineage.transport.url=http://marquez-api:5000 \
  --conf spark.openlineage.transport.endpoint=/api/v1/lineage \
  --conf spark.openlineage.namespace=loan-risk-demo \
  /opt/project/spark/batch_pipeline.py

Marquez UI: http://localhost:3001

Contributor

Nishitha S
PES1UG24AM180
PES University
