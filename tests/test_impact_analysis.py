from unittest.mock import patch

from lineage.impact_analysis import (
    build_lineage_graph,
    find_downstream_impact,
)


FAKE_JOBS = [
    {"name": "loan_data_validation"},
    {"name": "loan_model_training"},
    {"name": "loan_prediction_generation"},
    {"name": "loan_fairness_dashboard"},
    {"name": "lineage-connectivity-test"},
]


FAKE_DETAILS = {
    "loan_data_validation": {
        "inputs": [{"name": "synthetic_loan_applications"}],
        "outputs": [{"name": "processed_loan_applications"}],
    },
    "loan_model_training": {
        "inputs": [{"name": "processed_loan_applications"}],
        "outputs": [{"name": "loan_approval_model"}],
    },
    "loan_prediction_generation": {
        "inputs": [
            {"name": "loan_approval_model"},
            {"name": "processed_loan_applications"},
        ],
        "outputs": [{"name": "loan_predictions"}],
    },
    "loan_fairness_dashboard": {
        "inputs": [{"name": "loan_predictions"}],
        "outputs": [{"name": "approval_dashboard"}],
    },
    "lineage-connectivity-test": {
        "inputs": [{"name": "synthetic_loan_applications"}],
        "outputs": [{"name": "temporary_dataset"}],
    },
}


def fake_get_job_details(job_name):
    return FAKE_DETAILS[job_name]


@patch("lineage.impact_analysis.get_job_details", side_effect=fake_get_job_details)
def test_build_lineage_graph(mock_details):
    jobs = FAKE_JOBS[:4]

    graph = build_lineage_graph(jobs)

    assert "processed_loan_applications" in graph["synthetic_loan_applications"]
    assert "loan_approval_model" in graph["processed_loan_applications"]
    assert "loan_predictions" in graph["loan_approval_model"]
    assert "approval_dashboard" in graph["loan_predictions"]


@patch("lineage.impact_analysis.get_job_details", side_effect=fake_get_job_details)
def test_full_downstream_impact(mock_details):
    jobs = FAKE_JOBS[:4]

    result = find_downstream_impact(
        "synthetic_loan_applications",
        jobs,
    )

    assert set(result["affected_datasets"]) == {
        "processed_loan_applications",
        "loan_approval_model",
        "loan_predictions",
        "approval_dashboard",
    }

    assert result["affected_models"] == ["loan_approval_model"]
    assert result["affected_dashboards"] == ["approval_dashboard"]

    assert len(result["affected_jobs"]) == 4


@patch("lineage.impact_analysis.get_job_details", side_effect=fake_get_job_details)
def test_no_duplicate_datasets(mock_details):
    result = find_downstream_impact(
        "synthetic_loan_applications",
        FAKE_JOBS[:4],
    )

    datasets = result["affected_datasets"]

    assert len(datasets) == len(set(datasets))


@patch("lineage.impact_analysis.get_job_details", side_effect=fake_get_job_details)
def test_disconnected_dataset_has_no_impact(mock_details):
    result = find_downstream_impact(
        "unrelated_dataset",
        FAKE_JOBS[:4],
    )

    assert result["affected_datasets"] == []
    assert result["affected_jobs"] == []


@patch("lineage.impact_analysis.get_job_details", side_effect=fake_get_job_details)
def test_connectivity_test_job_is_not_included(mock_details):
    jobs = [
        job for job in FAKE_JOBS
        if job["name"] != "lineage-connectivity-test"
    ]

    result = find_downstream_impact(
        "synthetic_loan_applications",
        jobs,
    )

    job_names = {
        job["name"]
        for job in result["affected_jobs"]
    }

    assert "lineage-connectivity-test" not in job_names