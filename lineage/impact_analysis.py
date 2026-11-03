"""
Downstream impact analysis using Marquez lineage metadata.

Identifies downstream datasets, affected jobs, models, and dashboards.
Excludes temporary connectivity-test jobs.
"""

import argparse
import json
import os
from collections import deque
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MARQUEZ_API = os.getenv(
    "MARQUEZ_API",
    "http://localhost:5002/api/v1",
)

NAMESPACE = os.getenv(
    "OPENLINEAGE_NAMESPACE",
    "loan-risk-demo",
)

EXCLUDED_JOBS = {
    "lineage-connectivity-test",
}


def get_json(endpoint):
    url = "{}/{}".format(
        MARQUEZ_API.rstrip("/"),
        endpoint.lstrip("/"),
    )

    request = Request(
        url,
        headers={"Accept": "application/json"},
    )

    try:
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    except HTTPError as error:
        raise RuntimeError(
            "Marquez API returned HTTP {} for {}".format(
                error.code,
                url,
            )
        ) from error

    except URLError as error:
        raise RuntimeError(
            "Could not connect to Marquez: {}".format(error.reason)
        ) from error


def get_all_jobs():
    data = get_json(
        "namespaces/{}/jobs".format(NAMESPACE)
    )

    return [
        job
        for job in data.get("jobs", [])
        if job.get("name") not in EXCLUDED_JOBS
    ]


def get_job_details(job_name):
    endpoint = "namespaces/{}/jobs/{}".format(
        NAMESPACE,
        job_name,
    )

    return get_json(endpoint)


def build_lineage_graph(jobs):
    """Build a dataset dependency graph from Marquez job metadata."""

    graph = {}

    for job in jobs:
        details = get_job_details(job["name"])

        inputs = details.get("inputs", [])
        outputs = details.get("outputs", [])

        for input_dataset in inputs:
            input_name = input_dataset["name"]

            graph.setdefault(input_name, set())

            for output_dataset in outputs:
                output_name = output_dataset["name"]
                graph.setdefault(output_name, set())

                graph[input_name].add(output_name)

    return graph


def find_downstream_impact(source_dataset, jobs):
    """Find every unique dataset reachable from the source dataset."""

    graph = build_lineage_graph(jobs)

    visited = {source_dataset}
    affected_datasets = []
    queue = deque([source_dataset])

    while queue:
        current_dataset = queue.popleft()

        for downstream_dataset in sorted(
            graph.get(current_dataset, set())
        ):
            if downstream_dataset not in visited:
                visited.add(downstream_dataset)
                affected_datasets.append(downstream_dataset)
                queue.append(downstream_dataset)

    affected_jobs = []

    for job in jobs:
        details = get_job_details(job["name"])

        input_names = {
            item["name"]
            for item in details.get("inputs", [])
        }

        output_names = {
            item["name"]
            for item in details.get("outputs", [])
        }

        if input_names.intersection(visited):
            affected_jobs.append({
                "name": job["name"],
                "inputs": sorted(input_names),
                "outputs": sorted(output_names),
            })

    affected_models = [
        name for name in affected_datasets
        if "model" in name.lower()
    ]

    affected_dashboards = [
        name for name in affected_datasets
        if "dashboard" in name.lower()
    ]

    return {
        "source_dataset": source_dataset,
        "affected_datasets": affected_datasets,
        "affected_models": affected_models,
        "affected_dashboards": affected_dashboards,
        "affected_jobs": affected_jobs,
    }


def print_report(result):
    print("\n" + "=" * 60)
    print("        DATASET DOWNSTREAM IMPACT ANALYSIS")
    print("=" * 60)

    print("\nFlagged source dataset:")
    print("  {}".format(result["source_dataset"]))

    sections = [
        ("Affected datasets", "affected_datasets"),
        ("Affected models", "affected_models"),
        ("Affected dashboards", "affected_dashboards"),
    ]

    for title, key in sections:
        print("\n{}:".format(title))

        if result[key]:
            for item in result[key]:
                print("  -> {}".format(item))
        else:
            print("  None identified.")

    print("\nAffected jobs:")

    if result["affected_jobs"]:
        for job in result["affected_jobs"]:
            print("\n  Job: {}".format(job["name"]))
            print("  Inputs: {}".format(
                ", ".join(job["inputs"]) or "None"
            ))
            print("  Outputs: {}".format(
                ", ".join(job["outputs"]) or "None"
            ))
    else:
        print("  None identified.")

    print("\nSummary:")
    print("  Total affected datasets: {}".format(
        len(result["affected_datasets"])
    ))
    print("  Total affected models: {}".format(
        len(result["affected_models"])
    ))
    print("  Total affected dashboards: {}".format(
        len(result["affected_dashboards"])
    ))
    print("  Total affected jobs: {}".format(
        len(result["affected_jobs"])
    ))

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze downstream impact of a dataset."
    )

    parser.add_argument(
        "--dataset",
        default="synthetic_loan_applications",
    )

    parser.add_argument(
        "--output",
        default=None,
    )

    args = parser.parse_args()

    try:
        jobs = get_all_jobs()

        result = find_downstream_impact(
            args.dataset,
            jobs,
        )

        print_report(result)

        if args.output:
            directory = os.path.dirname(args.output)

            if directory:
                os.makedirs(directory, exist_ok=True)

            with open(args.output, "w", encoding="utf-8") as file:
                json.dump(result, file, indent=4)

            print("\nJSON report saved to: {}".format(args.output))

    except (RuntimeError, KeyError, ValueError) as error:
        print("\nImpact analysis failed: {}".format(error))
        raise SystemExit(1)


if __name__ == "__main__":
    main()