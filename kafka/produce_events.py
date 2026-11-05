
"""
Generate synthetic loan application events and publish them to Kafka.

Usage:
    python kafka/produce_events.py
"""

import json
import os
import random
import time
import uuid
import math
from datetime import datetime, timezone

from kafka import KafkaProducer


# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "loan_application_events"
)

random.seed(42)


def generate_loan_event():
    """Generate one synthetic loan application event."""

    # Generate applicant information
    age = random.randint(21, 65)
    annual_income = random.randint(200000, 2000000)
    credit_score = random.randint(300, 850)
    loan_amount = random.randint(50000, 1000000)
    employment_years = random.randint(0, 35)

    # Synthetic group for the controlled fairness experiment
    group = random.choice(["A", "B"])

    # Calculate unbiased ground-truth eligibility
    base_score = (
        0.003 * (credit_score - 600)
        + 0.000001 * (annual_income - 500000)
        - 0.000002 * (loan_amount - 200000)
        + 0.04 * employment_years
    )

    eligibility_probability = 1 / (
        1 + math.exp(-base_score)
    )

    eligible = int(
        random.random() < eligibility_probability
    )

    # Introduce controlled group-dependent bias
    score = base_score + (
        0.8 if group == "A" else -0.8
    )

    approval_probability = 1 / (
        1 + math.exp(-score)
    )

    approved = int(
        random.random() < approval_probability
    )

    # Construct the Kafka event
    event = {
        "event_id": str(uuid.uuid4()),
        "application_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "age": age,
        "annual_income": annual_income,
        "credit_score": credit_score,
        "loan_amount": loan_amount,
        "employment_years": employment_years,
        "synthetic_group": group,
        "eligible": eligible,
        "approved": approved
    }

    return event


def main():
    print(
        f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}..."
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )

    print(f"Publishing events to topic: {KAFKA_TOPIC}")

    try:
        while True:
            event = generate_loan_event()

            future = producer.send(
                KAFKA_TOPIC,
                value=event
            )

            metadata = future.get(timeout=10)

            print(
                f"Published application {event['application_id']} "
                f"to {metadata.topic}, "
                f"partition {metadata.partition}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping event producer...")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
