import pytest

from responsible_ai.fairness_metrics import (
    calculate_group_metrics,
    calculate_fairness_metrics,
)


def test_group_metrics_calculation():
    y_true = [1, 1, 0, 0, 1, 0]
    y_pred = [1, 0, 0, 0, 1, 1]
    groups = ["A", "A", "A", "B", "B", "B"]

    result = calculate_group_metrics(y_true, y_pred, groups)

    assert result["A"]["sample_count"] == 3
    assert result["B"]["sample_count"] == 3

    assert result["A"]["predicted_approval_rate"] == pytest.approx(1 / 3)
    assert result["B"]["predicted_approval_rate"] == pytest.approx(2 / 3)


def test_demographic_parity_difference():
    y_true = [1, 1, 0, 0]
    y_pred = [1, 1, 0, 1]
    groups = ["A", "A", "B", "B"]

    result = calculate_fairness_metrics(y_true, y_pred, groups)

    assert result["demographic_parity_difference"] == pytest.approx(0.5)


def test_demographic_parity_ratio():
    y_true = [1, 1, 0, 0]
    y_pred = [1, 1, 0, 1]
    groups = ["A", "A", "B", "B"]

    result = calculate_fairness_metrics(y_true, y_pred, groups)

    assert result["demographic_parity_ratio"] == pytest.approx(0.5)


def test_equal_opportunity_difference():
    y_true = [1, 1, 1, 1]
    y_pred = [1, 1, 1, 0]
    groups = ["A", "A", "B", "B"]

    result = calculate_fairness_metrics(y_true, y_pred, groups)

    assert result["equal_opportunity_difference"] == pytest.approx(0.5)


def test_mismatched_input_lengths():
    with pytest.raises(ValueError):
        calculate_fairness_metrics(
            [1, 0],
            [1],
            ["A", "B"],
        )


def test_empty_inputs():
    with pytest.raises(ValueError):
        calculate_fairness_metrics([], [], [])


def test_invalid_binary_values():
    with pytest.raises(ValueError):
        calculate_fairness_metrics(
            [1, 2],
            [1, 0],
            ["A", "B"],
        )