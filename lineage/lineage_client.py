"""
Explicit OpenLineage event client for Marquez.

Sends START and COMPLETE events directly to the Marquez
OpenLineage API using Python's standard library.
"""

import json
import os
import uuid

from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


# Marquez API configuration.
MARQUEZ_URL = os.getenv(
    "OPENLINEAGE_URL",
    "http://marquez-api:5000/api/v1/lineage",
)

NAMESPACE = os.getenv(
    "OPENLINEAGE_NAMESPACE",
    "loan-risk-demo",
)

PRODUCER = "https://openlineage.io"


def current_timestamp():
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def send_event(event):
    """
    Send one OpenLineage event to Marquez.

    Raises an exception if Marquez cannot accept the event.
    """

    payload = json.dumps(event).encode("utf-8")

    request = Request(
        MARQUEZ_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=10) as response:
            response_body = response.read().decode("utf-8")

            print(
                "[OpenLineage] {} event sent successfully. HTTP {}".format(
                    event["eventType"],
                    response.status,
                )
            )

            return response_body

    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")

        raise RuntimeError(
            "Marquez rejected the OpenLineage event. "
            "HTTP {}: {}".format(error.code, error_body)
        ) from error

    except URLError as error:
        raise RuntimeError(
            "Could not connect to Marquez at {}: {}".format(
                MARQUEZ_URL,
                error.reason,
            )
        ) from error


def build_dataset(dataset_name, namespace=None):
    """Build an OpenLineage dataset reference."""

    return {
        "namespace": namespace or NAMESPACE,
        "name": dataset_name,
    }


def build_event(
    event_type,
    job_name,
    run_id,
    inputs=None,
    outputs=None,
    namespace=None,
):
    """Construct an OpenLineage RunEvent."""

    return {
        "eventType": event_type,
        "eventTime": current_timestamp(),
        "run": {
            "runId": run_id,
        },
        "job": {
            "namespace": namespace or NAMESPACE,
            "name": job_name,
        },
        "inputs": [
            build_dataset(name)
            for name in (inputs or [])
        ],
        "outputs": [
            build_dataset(name)
            for name in (outputs or [])
        ],
        "producer": PRODUCER,
        "schemaURL": (
            "https://openlineage.io/spec/2-0-2/"
            "OpenLineage.json#/$defs/RunEvent"
        ),
    }


def emit_job_lineage(
    job_name,
    inputs=None,
    outputs=None,
    run_id=None,
):
    """
    Record a completed pipeline stage in Marquez.

    Sends START and COMPLETE events with the same run ID.

    Args:
        job_name: Unique name of the pipeline stage.
        inputs: List of input dataset names.
        outputs: List of output dataset names.
        run_id: Optional run ID. A UUID is generated if omitted.

    Returns:
        The run ID used for both events.
    """

    run_id = run_id or str(uuid.uuid4())

    start_event = build_event(
        event_type="START",
        job_name=job_name,
        run_id=run_id,
        inputs=inputs,
        outputs=outputs,
    )

    send_event(start_event)

    complete_event = build_event(
        event_type="COMPLETE",
        job_name=job_name,
        run_id=run_id,
        inputs=inputs,
        outputs=outputs,
    )

    send_event(complete_event)

    print(
        "[OpenLineage] Recorded job: {}".format(job_name)
    )

    return run_id