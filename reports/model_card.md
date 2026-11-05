# Model Card: Synthetic Loan Approval Prediction Model

**Project:** Data Lineage and Responsible AI Using OpenLineage  
**Model:** Loan Approval Logistic Regression Classifier  
**Framework:** Apache Spark ML  
**Version:** 1.0  
**Model Type:** Binary Classification  
**Dataset:** Synthetic Loan Application Dataset  
**Status:** Experimental / Academic Demonstration

---

## 1. Model Overview

The Loan Approval Prediction Model is a binary classification model developed using Apache Spark ML.

It predicts whether a synthetic loan application should be approved based on applicant-related features such as age, annual income, credit score, loan amount, and employment years.

The model is part of an end-to-end data lineage system designed to demonstrate how machine learning models and downstream assets can be traced when an upstream dataset is identified as potentially biased.

The model is intended exclusively for academic experimentation and responsible AI demonstrations. It is not designed or approved for actual lending decisions.

## 2. Intended Use

### Primary Intended Uses

- Demonstrating binary classification using Apache Spark ML.
- Evaluating model performance on synthetic loan application data.
- Comparing group-level prediction outcomes.
- Demonstrating fairness evaluation using demographic parity and equal opportunity.
- Demonstrating downstream impact analysis using OpenLineage and Marquez.

### Out-of-Scope Uses

The model must not be used for:

- Real-world loan approval or rejection.
- Creditworthiness assessment of actual individuals.
- Automated financial decisions.
- Production lending systems.
- Decisions affecting access to financial services.

## 3. Training Data

The model was trained using a synthetically generated loan application dataset.

| Attribute | Description |
|---|---|
| Dataset size | 5,000 records |
| Data type | Synthetic |
| Target variable | Loan approval |
| Protected-group proxy | Synthetic group label |
| Groups | A and B |
| Training records | 4,042 |
| Test records | 958 |
| Invalid records | 0 |

### Input Features

The synthetic dataset contains attributes including:

- Age
- Annual income
- Credit score
- Loan amount
- Employment years
- Synthetic group
- Loan approval label

The dataset includes intentionally introduced group-dependent approval patterns to support controlled fairness experiments.

No real personal or financial information was used.

## 4. Model Development

The model was developed using Apache Spark ML.

The batch processing workflow includes:

1. Synthetic data generation.
2. Data validation and preprocessing.
3. Feature assembly and transformation.
4. Logistic regression model training.
5. Prediction generation.
6. Performance and fairness evaluation.

The trained model and its outputs are represented as downstream assets in the OpenLineage metadata graph.

## 5. Performance Evaluation

The model was evaluated on a held-out test dataset containing 958 records.

| Metric | Result |
|---|---:|
| Test Accuracy | 89.77% |

Accuracy measures the proportion of test predictions that match the actual labels.

Although the model achieves approximately 89.77% test accuracy, accuracy alone does not establish that its predictions are fair or appropriate.

## 6. Fairness Evaluation

Fairness was evaluated by comparing outcomes between synthetic groups A and B.

### Measured Results

| Metric | Group A | Group B |
|---|---:|---:|
| Test applications | 470 | 488 |
| Actual approval rate | 66.81% | 59.02% |
| Predicted approval rate | 68.30% | 67.01% |
| True positive rate | 63.83% | 56.56% |

### Overall Fairness Indicators

| Metric | Result |
|---|---:|
| Actual approval-rate gap | 7.79 percentage points |
| Predicted approval-rate gap | 1.29 percentage points |
| Demographic parity ratio | 0.9811 |
| Equal opportunity difference | Approximately 7.27 percentage points |

### Interpretation

The predicted approval rates differ by approximately 1.29 percentage points between the two groups.

The demographic parity ratio of approximately 0.9811 indicates that the predicted approval rates are relatively close in this test sample.

However, the true positive rates differ by approximately 7.27 percentage points, indicating a difference in the proportion of eligible applicants correctly approved.

These metrics measure different aspects of fairness. A model may have similar overall approval rates while still exhibiting differences in error rates or opportunities across groups.

Therefore, the results should not be interpreted as proof that the model is completely fair.

## 7. Data Lineage and Traceability

The model is integrated into an OpenLineage-instrumented batch pipeline.

The lineage graph records the following dependencies:

```text
synthetic_loan_applications
          |
          v
processed_loan_applications
          |
          v
loan_approval_model
          |
          v
loan_predictions
          |
          v
approval_dashboard

Marquez provides a visual representation of these dependencies.

If the original synthetic dataset is identified as potentially biased, the impact analysis component can identify downstream assets that require investigation.

The identified downstream assets include:

- Processed loan application dataset.
- Trained loan approval model.
- Generated loan predictions.
- Approval dashboard.

This supports traceability and impact assessment during data quality and responsible AI investigations.

## 8. Limitations and Risks

1. **Synthetic data:** The dataset does not represent real-world loan applicant populations.
2. **Limited evaluation:** Results are based on a single held-out test split.
3. **Group definitions:** Groups A and B are synthetic proxies used only for experimentation.
4. **Fairness limitations:** Demographic parity and equal opportunity do not capture every dimension of fairness.
5. **Distribution sensitivity:** Model performance and fairness indicators may change when the data distribution changes.
6. **No production validation:** The model has not undergone real-world lending validation, regulatory review, or deployment testing.
7. **Lineage limitations:** Recorded dependencies support impact analysis but do not automatically retrain or disable affected models.

## 9. Ethical Considerations

Loan approval is a high-impact decision that can affect access to financial resources.

The project therefore uses synthetic data and does not make recommendations about actual individuals.

The fairness analysis demonstrates how group-level disparities can be measured and investigated.

In a real lending environment, fairness evaluation would require appropriate legal and ethical review, carefully justified group definitions, representative datasets, privacy protections, and human oversight.

## 10. Recommendations

Before considering any real-world application, further work would be required:

- Evaluate the model on representative and appropriately governed datasets.
- Conduct more extensive fairness testing.
- Investigate differences in false positive and false negative rates.
- Assess model robustness under changing data distributions.
- Introduce human review and appropriate governance controls.
- Establish procedures for investigating and responding to upstream data-quality issues.

## 11. Conclusion

The model demonstrates how predictive performance, fairness evaluation, and data lineage can be integrated into a machine learning workflow.

The experiment highlights that model accuracy alone is insufficient for responsible AI assessment.

By recording upstream and downstream dependencies using OpenLineage and Marquez, the project provides a foundation for identifying models and analytical assets that may be affected when source data is flagged for investigation.


