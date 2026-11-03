"""Generate synthetic loan applications with controlled approval bias."""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def generate_data(rows: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Generate artificial loan data with an intentional group disparity."""
    if rows < 100:
        raise ValueError("rows must be at least 100")

    rng = np.random.default_rng(seed)

    age = rng.integers(21, 66, size=rows)
    income = rng.integers(20000, 180001, size=rows)
    credit_score = rng.integers(300, 851, size=rows)
    loan_amount = rng.integers(5000, 100001, size=rows)
    employment_years = np.minimum(
        rng.integers(0, 31, size=rows),
        np.maximum(age - 18, 0),
    )

    # Synthetic group attribute for a controlled fairness experiment.
    # Group A receives a higher approval probability by design.
    group = rng.choice(["A", "B"], size=rows, p=[0.5, 0.5])

    affordability = income / np.maximum(loan_amount, 1)
    score = (
        0.035 * (credit_score - 600)
        + 0.000012 * (income - 60000)
        + 0.12 * employment_years
        + 1.2 * (affordability - 1.0)
    )

    # Deliberately introduce group-dependent bias.
    score += np.where(group == "A", 0.8, -0.8)

    probability = 1.0 / (1.0 + np.exp(-np.clip(score, -20, 20)))
    approved = rng.binomial(1, probability)

    return pd.DataFrame(
        {
            "application_id": [f"APP{i:06d}" for i in range(1, rows + 1)],
            "age": age,
            "annual_income": income,
            "credit_score": credit_score,
            "loan_amount": loan_amount,
            "employment_years": employment_years,
            "synthetic_group": group,
            "approved": approved,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="data/synthetic_loan_applications.csv")
    args = parser.parse_args()

    df = generate_data(rows=args.rows, seed=args.seed)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)

    print(f"Generated {len(df):,} synthetic loan applications")
    print(f"Saved to: {output}")
    print("\nApproval rate by synthetic group:")
    print(df.groupby("synthetic_group")["approved"].mean().round(3))
    print("\nDataset preview:")
    print(df.head())


if __name__ == "__main__":
    main()