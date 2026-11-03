"""
Spark batch pipeline for the synthetic loan approval project.

Processes synthetic loan data, trains a classifier, generates predictions,
calculates fairness metrics, and records explicit OpenLineage events
in Marquez.
"""

import json
import os
import sys

from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------
# PROJECT CONFIGURATION
# ---------------------------------------------------------

PROJECT_ROOT = "/opt/project"

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from lineage.lineage_client import emit_job_lineage


INPUT_PATH = os.path.join(
    PROJECT_ROOT, "data", "synthetic_loan_applications.csv"
)

PROCESSED_PATH = os.path.join(
    PROJECT_ROOT, "data", "processed_loan_applications"
)

PREDICTIONS_PATH = os.path.join(
    PROJECT_ROOT, "data", "loan_predictions"
)

MODEL_PATH = os.path.join(
    PROJECT_ROOT, "data", "loan_model"
)

FAIRNESS_PATH = os.path.join(
    PROJECT_ROOT, "data", "approval_dashboard"
)

METRICS_PATH = os.path.join(
    PROJECT_ROOT, "reports", "batch_metrics.json"
)


FEATURE_COLUMNS = [
    "age",
    "annual_income",
    "credit_score",
    "loan_amount",
    "employment_years",
]


# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("loan-batch-lineage-pipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n=== LOAN BATCH LINEAGE PIPELINE ===")

        # -------------------------------------------------
        # 1. READ SOURCE DATA
        # -------------------------------------------------

        print("\n[1] Reading synthetic loan applications")

        source = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(INPUT_PATH)
        )

        required_columns = [
            "application_id",
            "age",
            "annual_income",
            "credit_score",
            "loan_amount",
            "employment_years",
            "synthetic_group",
            "approved",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in source.columns
        ]

        if missing_columns:
            raise ValueError(
                "Missing required columns: {}".format(missing_columns)
            )

        source_count = source.count()

        if source_count == 0:
            raise ValueError("The input dataset is empty.")

        print("Source rows:", source_count)

        # -------------------------------------------------
        # 2. VALIDATE AND CLEAN DATA
        # -------------------------------------------------

        print("\n[2] Validating and cleaning data")

        valid = source.dropna(subset=required_columns)

        for column in FEATURE_COLUMNS:
            valid = valid.filter(F.col(column).isNotNull())

        valid = valid.filter(
            (F.col("age") >= 18)
            & (F.col("annual_income") > 0)
            & (F.col("credit_score") >= 300)
            & (F.col("credit_score") <= 850)
            & (F.col("loan_amount") > 0)
            & (F.col("employment_years") >= 0)
            & (F.col("approved").isin(0, 1))
            & (F.col("synthetic_group").isin("A", "B"))
        )

        valid_count = valid.count()

        if valid_count < 20:
            raise ValueError(
                "Not enough valid rows to train the model: {}".format(
                    valid_count
                )
            )

        rejected_count = source_count - valid_count

        print("Valid rows:", valid_count)
        print("Rejected rows:", rejected_count)

        # Save processed dataset.

        (
            valid.write
            .mode("overwrite")
            .parquet(PROCESSED_PATH)
        )

        print("Processed data saved.")

        # Record data cleaning lineage.

        emit_job_lineage(
            job_name="loan_data_validation",
            inputs=["synthetic_loan_applications"],
            outputs=["processed_loan_applications"],
        )

        # -------------------------------------------------
        # 3. TRAIN / TEST SPLIT
        # -------------------------------------------------

        print("\n[3] Creating training and testing datasets")

        train, test = valid.randomSplit(
            [0.8, 0.2],
            seed=42
        )

        train_count = train.count()
        test_count = test.count()

        if train_count == 0 or test_count == 0:
            raise ValueError("Training or test split is empty.")

        print("Training rows:", train_count)
        print("Test rows:", test_count)

        # -------------------------------------------------
        # 4. MODEL TRAINING
        # -------------------------------------------------

        print("\n[4] Training logistic regression model")

        assembler = VectorAssembler(
            inputCols=FEATURE_COLUMNS,
            outputCol="raw_features",
            handleInvalid="error",
        )

        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withMean=True,
            withStd=True,
        )

        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="approved",
            predictionCol="prediction",
            probabilityCol="probability",
            maxIter=50,
            regParam=0.01,
        )

        pipeline = Pipeline(
            stages=[
                assembler,
                scaler,
                classifier,
            ]
        )

        model = pipeline.fit(train)

        model.write().overwrite().save(MODEL_PATH)

        print("Trained model saved.")

        # Record model training lineage.

        emit_job_lineage(
            job_name="loan_model_training",
            inputs=["processed_loan_applications"],
            outputs=["loan_approval_model"],
        )

        # -------------------------------------------------
        # 5. MODEL EVALUATION
        # -------------------------------------------------

        print("\n[5] Evaluating model")

        predictions = model.transform(test)

        evaluation = predictions.select(
            F.col("approved").cast("double").alias("label"),
            F.col("prediction").cast("double").alias("prediction"),
        )

        total_test = evaluation.count()

        correct = evaluation.filter(
            F.col("label") == F.col("prediction")
        ).count()

        accuracy = float(correct) / float(total_test)

        print("Test accuracy: {:.4f}".format(accuracy))

        # -------------------------------------------------
        # 6. GENERATE PREDICTIONS
        # -------------------------------------------------

        print("\n[6] Generating predictions")

        prediction_output = (
            predictions
            .withColumn(
                "approval_probability",
                vector_to_array(F.col("probability"))[1],
            )
            .select(
                "application_id",
                "synthetic_group",
                "approved",
                "prediction",
                "approval_probability",
            )
        )

        (
            prediction_output.write
            .mode("overwrite")
            .option("header", True)
            .csv(PREDICTIONS_PATH)
        )

        print("Predictions saved.")

        # Record prediction lineage.

        emit_job_lineage(
            job_name="loan_prediction_generation",
            inputs=[
                "processed_loan_applications",
                "loan_approval_model",
            ],
            outputs=["loan_predictions"],
        )

        # -------------------------------------------------
        # 7. FAIRNESS ANALYSIS
        # -------------------------------------------------

        print("\n[7] Calculating group fairness metrics")

        fairness = (
            predictions
            .groupBy("synthetic_group")
            .agg(
                F.count("*").alias("applications"),

                F.avg(
                    F.col("approved").cast("double")
                ).alias("actual_approval_rate"),

                F.avg(
                    F.col("prediction").cast("double")
                ).alias("predicted_approval_rate"),

                F.avg(
                    F.when(
                        (F.col("approved") == 1)
                        & (F.col("prediction") == 1),
                        1.0,
                    ).otherwise(0.0)
                ).alias("true_positive_rate"),
            )
            .orderBy("synthetic_group")
        )

        fairness_rows = [
            row.asDict(recursive=True)
            for row in fairness.collect()
        ]

        (
            fairness.write
            .mode("overwrite")
            .option("header", True)
            .csv(FAIRNESS_PATH)
        )

        print("\nGroup fairness results:")

        for row in fairness_rows:
            print(
                json.dumps(
                    row,
                    sort_keys=True,
                    default=str
                )
            )

        # Record dashboard lineage.

        emit_job_lineage(
            job_name="loan_fairness_dashboard",
            inputs=["loan_predictions"],
            outputs=["approval_dashboard"],
        )

        # -------------------------------------------------
        # 8. FAIRNESS METRICS
        # -------------------------------------------------

        actual_rates = {
            row["synthetic_group"]: row["actual_approval_rate"]
            for row in fairness_rows
        }

        predicted_rates = {
            row["synthetic_group"]: row["predicted_approval_rate"]
            for row in fairness_rows
        }

        actual_disparity = None
        predicted_disparity = None
        demographic_parity_ratio = None

        if "A" in actual_rates and "B" in actual_rates:

            actual_disparity = abs(
                actual_rates["A"] - actual_rates["B"]
            )

            predicted_disparity = abs(
                predicted_rates["A"] - predicted_rates["B"]
            )

            higher_rate = max(
                predicted_rates["A"],
                predicted_rates["B"]
            )

            lower_rate = min(
                predicted_rates["A"],
                predicted_rates["B"]
            )

            if higher_rate > 0:
                demographic_parity_ratio = (
                    lower_rate / higher_rate
                )

        # -------------------------------------------------
        # 9. SAVE PIPELINE METRICS
        # -------------------------------------------------

        print("\n[8] Saving evaluation metrics")

        metrics = {
            "pipeline": "loan-batch-lineage-pipeline",

            "timestamp_utc": datetime.now(
                timezone.utc
            ).isoformat(),

            "input_rows": int(source_count),
            "valid_rows": int(valid_count),
            "rejected_rows": int(rejected_count),

            "training_rows": int(train_count),
            "test_rows": int(test_count),

            "test_accuracy": float(accuracy),

            "actual_approval_rate_gap": (
                float(actual_disparity)
                if actual_disparity is not None
                else None
            ),

            "predicted_approval_rate_gap": (
                float(predicted_disparity)
                if predicted_disparity is not None
                else None
            ),

            "demographic_parity_ratio": (
                float(demographic_parity_ratio)
                if demographic_parity_ratio is not None
                else None
            ),

            "fairness_by_group": fairness_rows,

            "artifacts": {
                "processed_data": PROCESSED_PATH,
                "model": MODEL_PATH,
                "predictions": PREDICTIONS_PATH,
                "fairness_dashboard_data": FAIRNESS_PATH,
            },
        }

        os.makedirs(
            os.path.dirname(METRICS_PATH),
            exist_ok=True
        )

        with open(METRICS_PATH, "w") as output_file:
            json.dump(
                metrics,
                output_file,
                indent=2,
                default=str
            )

        print("Metrics saved to:", METRICS_PATH)

        print("\n=== FINAL METRICS ===")
        print(json.dumps(metrics, indent=2, default=str))

        print("\n=== BATCH PIPELINE COMPLETED SUCCESSFULLY ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()