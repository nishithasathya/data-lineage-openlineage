"""
Fairness metrics for evaluating loan approval predictions.

The module calculates:
- Approval rates by group.
- Demographic parity difference.
- Demographic parity ratio.
- True positive rates by group.
- Equal opportunity difference.
"""

import numpy as np


def _validate_inputs(y_true, y_pred, groups):
    """Validate that all input arrays have matching lengths."""

    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    groups = np.asarray(groups)

    if not (len(y_true) == len(y_pred) == len(groups)):
        raise ValueError(
            "y_true, y_pred, and groups must have equal lengths."
        )

    if len(y_true) == 0:
        raise ValueError("Input arrays cannot be empty.")

    if not set(np.unique(y_true)).issubset({0, 1}):
        raise ValueError("y_true must contain only binary values (0, 1).")

    if not set(np.unique(y_pred)).issubset({0, 1}):
        raise ValueError("y_pred must contain only binary values (0, 1).")

    return y_true, y_pred, groups


def calculate_group_metrics(y_true, y_pred, groups):
    """Calculate approval rate, TPR, and sample count for each group."""

    y_true, y_pred, groups = _validate_inputs(
        y_true, y_pred, groups
    )

    results = {}

    for group in np.unique(groups):
        mask = groups == group

        actual = y_true[mask]
        predicted = y_pred[mask]

        approval_rate = float(np.mean(predicted))

        positive_cases = actual == 1

        if np.sum(positive_cases) == 0:
            true_positive_rate = None
        else:
            true_positive_rate = float(
                np.mean(predicted[positive_cases])
            )

        results[str(group)] = {
            "sample_count": int(np.sum(mask)),
            "actual_approval_rate": float(np.mean(actual)),
            "predicted_approval_rate": approval_rate,
            "true_positive_rate": true_positive_rate,
        }

    return results


def calculate_fairness_metrics(y_true, y_pred, groups):
    """
    Calculate demographic parity and equal opportunity metrics.

    Demographic parity difference:
        Maximum approval rate minus minimum approval rate.

    Demographic parity ratio:
        Minimum approval rate divided by maximum approval rate.

    Equal opportunity difference:
        Maximum TPR minus minimum TPR.
    """

    group_metrics = calculate_group_metrics(
        y_true, y_pred, groups
    )

    approval_rates = [
        item["predicted_approval_rate"]
        for item in group_metrics.values()
    ]

    tpr_values = [
        item["true_positive_rate"]
        for item in group_metrics.values()
        if item["true_positive_rate"] is not None
    ]

    max_approval = max(approval_rates)
    min_approval = min(approval_rates)

    demographic_parity_difference = (
        max_approval - min_approval
    )

    if max_approval == 0:
        demographic_parity_ratio = 1.0
    else:
        demographic_parity_ratio = (
            min_approval / max_approval
        )

    if len(tpr_values) >= 2:
        equal_opportunity_difference = (
            max(tpr_values) - min(tpr_values)
        )
    else:
        equal_opportunity_difference = None

    return {
        "group_metrics": group_metrics,
        "demographic_parity_difference": float(
            demographic_parity_difference
        ),
        "demographic_parity_ratio": float(
            demographic_parity_ratio
        ),
        "equal_opportunity_difference": (
            float(equal_opportunity_difference)
            if equal_opportunity_difference is not None
            else None
        ),
    }