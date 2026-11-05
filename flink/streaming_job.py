
import json
import uuid

from pyflink.common import WatermarkStrategy, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import AggregateFunction, MapFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows

from lineage_client import build_event, send_event


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "loan_application_events"


def validate_event(raw_event):
    """Validate an incoming loan application event."""

    event = json.loads(raw_event)

    required_fields = [
        "application_id",
        "event_timestamp",
        "age",
        "annual_income",
        "credit_score",
        "loan_amount",
        "employment_years",
        "synthetic_group",
        "eligible",
        "approved",
    ]

    missing = [
        field for field in required_fields
        if field not in event
    ]

    if missing:
        raise ValueError(
            "Missing required fields: {}".format(missing)
        )

    if event["synthetic_group"] not in ["A", "B"]:
        raise ValueError("Invalid synthetic group.")

    if event["eligible"] not in [0, 1]:
        raise ValueError("eligible must be 0 or 1.")

    if event["approved"] not in [0, 1]:
        raise ValueError("approved must be 0 or 1.")

    return json.dumps(event)


class ParseEvent(MapFunction):
    """Convert validated JSON strings into dictionaries."""

    def map(self, value):
        return json.loads(value)


class FairnessMetricsAggregate(AggregateFunction):
    """Calculate fairness metrics over a one-minute window."""

    def create_accumulator(self):
        return {
            "A": {
                "count": 0,
                "approved": 0,
                "eligible": 0,
                "eligible_approved": 0,
            },
            "B": {
                "count": 0,
                "approved": 0,
                "eligible": 0,
                "eligible_approved": 0,
            },
        }

    def add(self, event, accumulator):
        group = event["synthetic_group"]

        stats = accumulator[group]

        stats["count"] += 1
        stats["approved"] += event["approved"]
        stats["eligible"] += event["eligible"]

        if event["eligible"] == 1 and event["approved"] == 1:
            stats["eligible_approved"] += 1

        return accumulator

    def get_result(self, accumulator):

        group_metrics = {}

        for group, stats in accumulator.items():

            count = stats["count"]

            if count == 0:
                continue

            approval_rate = stats["approved"] / count

            if stats["eligible"] > 0:
                tpr = (
                    stats["eligible_approved"]
                    / stats["eligible"]
                )
            else:
                tpr = None

            group_metrics[group] = {
                "sample_count": count,
                "approval_rate": approval_rate,
                "true_positive_rate": tpr,
            }

        approval_rates = [
            item["approval_rate"]
            for item in group_metrics.values()
        ]

        tpr_values = [
            item["true_positive_rate"]
            for item in group_metrics.values()
            if item["true_positive_rate"] is not None
        ]

        if len(approval_rates) >= 2:

            dp_difference = (
                max(approval_rates) - min(approval_rates)
            )

            dp_ratio = (
                min(approval_rates) / max(approval_rates)
                if max(approval_rates) > 0
                else 1.0
            )

        else:
            dp_difference = None
            dp_ratio = None

        if len(tpr_values) >= 2:
            equal_opportunity_difference = (
                max(tpr_values) - min(tpr_values)
            )
        else:
            equal_opportunity_difference = None

        result = {
            "group_metrics": group_metrics,
            "demographic_parity_difference": dp_difference,
            "demographic_parity_ratio": dp_ratio,
            "equal_opportunity_difference": (
                equal_opportunity_difference
            ),
        }

        return json.dumps(result)

    def merge(self, accumulator, other_accumulator):

        for group in ["A", "B"]:
            for key in accumulator[group]:
                accumulator[group][key] += (
                    other_accumulator[group][key]
                )

        return accumulator


def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("loan-risk-flink-consumer-v2")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    loan_events = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Loan Application Source",
    )

    validated_events = loan_events.map(
        validate_event,
        output_type=Types.STRING()
    )

    validated_events.print("VALIDATED LOAN EVENTS")

    parsed_events = validated_events.map(
        ParseEvent(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    # Calculate fairness metrics over one-minute windows
    fairness_results = (
        parsed_events
        .window_all(
            TumblingProcessingTimeWindows.of(Time.minutes(1))
        )
        .aggregate(
            FairnessMetricsAggregate(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.STRING()
        )
    )

    fairness_results.print("STREAMING FAIRNESS METRICS")

    # Record the streaming job start in OpenLineage
    run_id = str(uuid.uuid4())
    with open("/opt/flink/streaming_run_id.txt", "w") as file:
      file.write(run_id)

    start_event = build_event(
        event_type="START",
        job_name="Loan Risk Streaming Pipeline",
        run_id=run_id,
        inputs=["loan_application_events"],
        outputs=["streaming_fairness_metrics"]
    )

    send_event(start_event)

    print("[OpenLineage] Streaming job START event recorded.")

    # Execute the streaming pipeline
    env.execute("Loan Risk Streaming Pipeline")

    complete_event = build_event(
        event_type="COMPLETE",
        job_name="Loan Risk Streaming Pipeline",
        run_id=run_id,
        inputs=["loan_application_events"],
        outputs=["streaming_fairness_metrics"]
    )

    send_event(complete_event)

    print("[OpenLineage] Streaming job COMPLETE event recorded.")


if __name__ == "__main__":
    main()
