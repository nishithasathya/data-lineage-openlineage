# Data Lineage with OpenLineage: Technical Report

## 1. Abstract

This project demonstrates how data lineage can be integrated into a machine learning workflow to improve transparency, traceability, and responsible AI practices.

The system uses Apache Spark for batch processing and model training, Apache Kafka for event ingestion, and Apache Flink for stream processing. OpenLineage captures data and job dependencies, while Marquez provides a centralized interface for visualizing lineage information.

A synthetic loan application dataset containing intentionally introduced group-level approval differences is used to demonstrate fairness evaluation and downstream impact analysis.

The batch model achieved a test accuracy of approximately 89.77%. Fairness metrics were calculated to examine differences in approval outcomes between synthetic groups. The impact analysis component successfully identified downstream datasets, models, predictions, and dashboards dependent on the original dataset.

## 2. Introduction

Machine learning systems depend on data that passes through multiple processing and transformation stages. Without proper tracking, it can be difficult to determine which models, predictions, or dashboards are affected when an upstream dataset is found to contain errors or potential bias.

Data lineage addresses this problem by recording the relationships between datasets, processing jobs, and downstream assets.

The primary objective of this project is to implement a data lineage system using OpenLineage and Marquez while demonstrating its relevance to Responsible AI.

The project focuses on two processing environments:

- **Batch processing:** Apache Spark processes synthetic loan application data, trains a model, generates predictions, and produces fairness dashboard data.
- **Stream processing:** Apache Kafka and Apache Flink process incoming loan application events and calculate group-level fairness metrics.

The system combines lineage tracking with fairness evaluation and downstream impact analysis.

## 3. System Architecture

### 3.1 Architecture Overview

The architecture consists of two independent processing pipelines connected to a common lineage backend.

**Batch pipeline:**

1. Synthetic loan application data is generated.
2. Apache Spark validates and preprocesses the data.
3. A Logistic Regression model is trained using Spark ML.
4. Predictions are generated using the trained model.
5. Fairness metrics and dashboard data are produced.
6. OpenLineage events record the relationships between pipeline stages.

**Streaming pipeline:**

1. A Kafka producer generates synthetic loan application events.
2. Events are published to the `loan_application_events` topic.
3. Apache Flink consumes and validates incoming events.
4. Events are processed in time-based windows.
5. Group-level approval and fairness metrics are calculated.
6. OpenLineage records the streaming job and its input and output datasets.

Marquez receives OpenLineage events from both pipelines and provides a visual representation of the recorded dependencies.

### 3.2 Technology Stack

| Technology | Purpose |
|---|---|
| Apache Spark 3.5.6 | Batch processing and model training |
| Apache Kafka 3.9.1 | Streaming event ingestion |
| Apache Flink 1.20.5 | Real-time stream processing |
| OpenLineage | Data lineage event collection |
| Marquez 0.50.0 | Lineage visualization and metadata management |
| Python | Pipeline implementation and analysis |
| PySpark ML | Logistic Regression model |
| Docker Compose | Container orchestration |
| Pytest | Automated testing |

### 3.3 Infrastructure

Docker Compose is used to run the infrastructure components in containers.

The environment includes:

- Marquez API and web interface.
- PostgreSQL metadata database.
- Apache Spark.
- Apache Kafka.
- Flink JobManager.
- Flink TaskManager.

This setup provides a reproducible environment for executing both batch and streaming workloads.

## 4. Implementation

### 4.1 Synthetic Dataset Generation

A synthetic loan application dataset was generated to simulate a lending scenario containing potential group-level approval bias.

The dataset contains 5,000 records with attributes such as:

- Application ID.
- Age.
- Annual income.
- Credit score.
- Loan amount.
- Employment years.
- Synthetic group.
- Approval outcome.

Two synthetic groups, A and B, were introduced to support fairness comparisons.

The dataset intentionally contains differences in approval outcomes between the groups. No real personal or financial information is used.

### 4.2 Batch Processing Using Apache Spark

The batch pipeline is implemented using PySpark.

It performs the following operations:

1. Reads the synthetic loan application dataset.
2. Validates the input records.
3. Prepares features for model training.
4. Splits the dataset into training and testing subsets.
5. Trains a Logistic Regression classifier.
6. Generates predictions on the test dataset.
7. Calculates performance and fairness metrics.
8. Produces output datasets for downstream analysis.

The pipeline processes all 5,000 input records successfully.

The dataset is divided into:

| Dataset | Records |
|---|---:|
| Training | 4,042 |
| Testing | 958 |
| Invalid records | 0 |
| Total | 5,000 |

### 4.3 Streaming Processing Using Kafka and Flink

The streaming pipeline demonstrates how incoming loan application events can be processed continuously.

A Python-based Kafka producer generates synthetic events and publishes them to the `loan_application_events` topic.

Apache Flink consumes these events and performs validation before calculating group-level metrics.

The streaming pipeline calculates:

- Number of applications per group.
- Approval rate.
- True positive rate.
- Demographic parity difference.
- Demographic parity ratio.
- Equal opportunity difference.

The metrics are calculated using tumbling processing-time windows of one minute.

The pipeline was executed successfully, and validated events and fairness metrics were observed in the Flink TaskManager logs.

### 4.4 OpenLineage and Marquez Integration

OpenLineage is used to record relationships between processing jobs and their input and output datasets.

The batch pipeline records the following lineage stages:

| Job | Input | Output |
|---|---|---|
| Data validation | `synthetic_loan_applications` | `processed_loan_applications` |
| Model training | `processed_loan_applications` | `loan_approval_model` |
| Prediction generation | `loan_approval_model`, `processed_loan_applications` | `loan_predictions` |
| Fairness dashboard | `loan_predictions` | `approval_dashboard` |

The streaming pipeline records:

| Job | Input | Output |
|---|---|---|
| Loan Risk Streaming Pipeline | `loan_application_events` | `streaming_fairness_metrics` |

The OpenLineage events are sent to the Marquez API.

The Marquez interface was used to verify the recorded batch and streaming dependencies. The streaming job was also verified with a completed run status.

## 5. Experimental Results

### 5.1 Batch Model Performance

The Logistic Regression model was evaluated using a held-out test dataset containing 958 records.

| Metric | Result |
|---|---:|
| Input records | 5,000 |
| Training records | 4,042 |
| Testing records | 958 |
| Invalid records | 0 |
| Test accuracy | 89.77% |

The model achieved an accuracy of approximately 89.77% on the test dataset.

However, predictive accuracy alone does not establish that a model is fair or suitable for a high-impact application.

### 5.2 Fairness Evaluation

Fairness metrics were calculated separately for synthetic groups A and B.

| Metric | Group A | Group B |
|---|---:|---:|
| Test applications | 470 | 488 |
| Actual approval rate | 66.81% | 59.02% |
| Predicted approval rate | 68.30% | 67.01% |
| True positive rate | 63.83% | 56.56% |

Overall fairness indicators:

| Metric | Result |
|---|---:|
| Actual approval rate gap | 7.79 percentage points |
| Predicted approval rate gap | 1.29 percentage points |
| Demographic parity ratio | 0.9811 |
| Equal opportunity difference | Approximately 7.27 percentage points |

The results show that the model's predicted approval rates are relatively close between the two synthetic groups.

However, the equal opportunity difference indicates that the true positive rates are not identical.

These results demonstrate why multiple fairness metrics should be considered instead of relying on a single measure.

The findings are limited to the synthetic dataset and should not be interpreted as evidence of fairness in real lending systems.

### 5.3 Streaming Fairness Results

The Flink pipeline successfully calculated fairness metrics over one-minute processing-time windows.

During execution, differences in approval rates and true positive rates between the synthetic groups were observed.

The values varied across windows because each window contained a different number and distribution of events.

These observations demonstrate how streaming fairness metrics can be monitored over time.

Individual windows should not be treated as conclusive fairness evaluations because their results are sensitive to sample size and event distribution.

## 6. Downstream Impact Analysis

A major objective of the project is to identify downstream assets when an upstream dataset is flagged for potential bias or data quality problems.

An impact analysis component was implemented using the Marquez REST API.

It retrieves lineage metadata and traverses the dependency graph to identify downstream assets.

When `synthetic_loan_applications` is selected as the source dataset, the system identifies the following affected assets:

1. `processed_loan_applications`
2. `loan_approval_model`
3. `loan_predictions`
4. `approval_dashboard`

The analysis also identifies the jobs responsible for creating these assets.

This demonstrates how lineage information can support investigations by identifying which models and analytical outputs may require review when their source data is flagged.

The implementation identifies dependencies but does not automatically retrain, disable, or modify affected models.

## 7. Testing and Validation

Automated tests were implemented using Pytest.

The test suite contains 17 tests covering three major areas.

### 7.1 Fairness Metric Tests

Tests verify:

- Group-level approval rates.
- Demographic parity difference.
- Demographic parity ratio.
- Equal opportunity difference.
- Input validation.
- Handling of empty inputs and mismatched array lengths.

### 7.2 Impact Analysis Tests

Tests verify:

- Downstream dependency traversal.
- Identification of affected datasets.
- Identification of affected models and dashboards.
- Duplicate prevention.
- Handling of disconnected datasets.
- Exclusion of temporary connectivity-test assets.

### 7.3 Lineage Coverage Tests

Tests verify the presence of batch and streaming pipeline components and check for expected lineage instrumentation.

The automated test suite completed successfully with:

**17 tests passed.**

In addition to automated tests, both batch and streaming pipelines were executed in the Docker environment.

The batch and streaming lineage relationships were verified through the Marquez interface.

## 8. Limitations and Risks

The project has several limitations:

1. **Synthetic data:** The dataset does not represent real-world loan applicant populations.
2. **Limited evaluation:** Model performance is based on a single held-out test split.
3. **Group definitions:** Groups A and B are synthetic proxies used only for experimentation.
4. **Fairness limitations:** Demographic parity and equal opportunity do not capture every dimension of fairness.
5. **Distribution sensitivity:** Streaming fairness metrics may fluctuate with changes in event distribution and sample size.
6. **No production validation:** The model has not undergone real-world lending validation, regulatory review, or deployment testing.
7. **Lineage limitations:** Recorded dependencies support impact analysis but do not automatically retrain or disable affected models.

## 9. Responsible AI and Ethical Considerations

Loan approval is a high-impact decision that can affect access to financial resources.

The project therefore uses synthetic data and does not make recommendations about actual individuals.

The fairness analysis demonstrates how group-level disparities can be measured and investigated.

Data lineage contributes to transparency by making dependencies between source datasets, processing jobs, models, predictions, and dashboards visible.

In a real lending environment, responsible deployment would require representative and appropriately governed datasets, privacy protections, extensive fairness testing, human oversight, and regulatory review.

## 10. Conclusion and Future Work

This project demonstrates how data lineage can be integrated into both batch and streaming machine learning workflows.

Apache Spark was used to process data and train a Logistic Regression model, while Apache Kafka and Apache Flink enabled continuous event processing and fairness monitoring.

OpenLineage and Marquez provided a mechanism for recording and visualizing dependencies across the pipelines.

The impact analysis component demonstrated how downstream datasets, models, predictions, and dashboards can be identified when an upstream dataset is flagged for investigation.

The experiment also highlights that model accuracy alone is insufficient for responsible AI assessment.

Future improvements could include:

- More extensive fairness evaluation across multiple datasets and time periods.
- Monitoring additional fairness indicators.
- Automated alerts when fairness metrics exceed defined thresholds.
- Integration of lineage-based alerts with model governance workflows.
- Automated identification of models requiring retraining or human review.
- Evaluation using larger and more representative datasets.

Overall, the project provides a reproducible demonstration of how Big Data processing, data lineage, and Responsible AI practices can work together to improve transparency and traceability in machine learning systems.