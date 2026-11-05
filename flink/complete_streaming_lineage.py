
"""Complete the saved Flink streaming lineage run."""

import os
import sys

sys.path.insert(0, "/opt/flink/lineage")

from lineage_client import build_event, send_event

JOB_NAME = "Loan Risk Streaming Pipeline"
INPUTS = ["loan_application_events"]
OUTPUTS = ["streaming_fairness_metrics"]
RUN_ID_FILE = "/opt/flink/streaming_run_id.txt"


def main():
    if not os.path.exists(RUN_ID_FILE):
        raise FileNotFoundError(
            "No saved run ID found. Start the Flink job first."
        )

    with open(RUN_ID_FILE, "r") as file:
        run_id = file.read().strip()

    if not run_id:
        raise ValueError("The saved run ID file is empty.")

    event = build_event(
        event_type="COMPLETE",
        job_name=JOB_NAME,
        run_id=run_id,
        inputs=INPUTS,
        outputs=OUTPUTS,
    )

    send_event(event)
    print(f"Completed OpenLineage run: {run_id}")

    os.remove(RUN_ID_FILE)


if __name__ == "__main__":
    main()
