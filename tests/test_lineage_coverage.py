from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read_project_file(relative_path):
    path = PROJECT_ROOT / relative_path

    assert path.exists(), "Missing project file: {}".format(relative_path)

    return path.read_text(encoding="utf-8")


def test_batch_pipeline_exists():
    read_project_file("spark/batch_pipeline.py")


def test_batch_pipeline_has_explicit_lineage():
    content = read_project_file("spark/batch_pipeline.py")

    assert "emit_job_lineage" in content
    assert "loan_data_validation" in content
    assert "loan_model_training" in content
    assert "loan_prediction_generation" in content
    assert "loan_fairness_dashboard" in content


def test_lineage_client_exists():
    content = read_project_file("lineage/lineage_client.py")

    assert "emit_job_lineage" in content
    assert "START" in content
    assert "COMPLETE" in content


def test_streaming_pipeline_files_exist():
    read_project_file("kafka/produce_events.py")
    read_project_file("flink/streaming_job.py")


def test_impact_analysis_exists():
    content = read_project_file("lineage/impact_analysis.py")

    assert "find_downstream_impact" in content
    assert "build_lineage_graph" in content