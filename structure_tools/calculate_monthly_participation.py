#!/usr/bin/env python3
"""Summarise per-participant survey submissions by calendar month.

This script inspects the structured CSV outputs (``initial_survey.csv`` and
``biweekly_survey.csv``) and counts how many qualifying survey responses each
participant submitted in every calendar month. Consent forms are excluded from
this view because compensation hinges on initial + biweekly activity only.

Outputs (written alongside the structured CSVs unless overridden):
    * ``monthly_participation_summary.csv`` – machine-readable table with one
      row per participant and dynamic columns for each ``YYYY-MM`` that appears
      in the dataset, plus a ``total_responses`` column.
    * ``monthly_participation_summary.md`` – Markdown table rendering of the
      same data for easy sharing.
    * Optional ``--latest-output`` file that is refreshed on every run so teams
      always have an up-to-date CSV outside the timestamped folder.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _coerce_datetime(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt


def _first_valid_string(series: pd.Series) -> str:
    for value in series:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _load_csv(path: Path, required: bool = True) -> pd.DataFrame:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required file not found: {path}")
        return pd.DataFrame()
    return pd.read_csv(path)


def _md_escape(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|")


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------


def compute_monthly_counts(structured_dir: Path) -> pd.DataFrame:
    consent_df = _load_csv(structured_dir / "consent.csv")
    if consent_df.empty:
        return pd.DataFrame()

    consent_df["submitted_at"] = _coerce_datetime(consent_df.get("submitted_at"))
    consent_df = consent_df.dropna(subset=["participant_uuid", "submitted_at"])

    consent_meta = (
        consent_df.sort_values("submitted_at")
        .groupby("participant_uuid")
        .agg(
            participant_code=("participant_signature", _first_valid_string),
        )
    )

    initial_df = _load_csv(structured_dir / "initial_survey.csv", required=False)
    biweekly_df = _load_csv(structured_dir / "biweekly_survey.csv", required=False)

    submissions: List[pd.DataFrame] = []

    if not initial_df.empty:
        initial_df["submitted_at"] = _coerce_datetime(initial_df.get("submitted_at"))
        submissions.append(
            initial_df.dropna(subset=["participant_uuid", "submitted_at"])[
                ["participant_uuid", "submitted_at"]
            ]
        )

    if not biweekly_df.empty:
        biweekly_df["submitted_at"] = _coerce_datetime(biweekly_df.get("submitted_at"))
        submissions.append(
            biweekly_df.dropna(subset=["participant_uuid", "submitted_at"])[
                ["participant_uuid", "submitted_at"]
            ]
        )

    if submissions:
        combined = pd.concat(submissions, ignore_index=True)
        combined = combined.sort_values("submitted_at")
        timestamps = combined["submitted_at"]
        if timestamps.dt.tz is not None:
            timestamps = timestamps.dt.tz_convert(None)
        combined["month"] = timestamps.dt.to_period("M").astype(str)
        pivot = (
            combined.groupby(["participant_uuid", "month"])
            .size()
            .unstack(fill_value=0)
        )
    else:
        pivot = pd.DataFrame()

    pivot = pivot.reindex(consent_meta.index, fill_value=0)
    month_columns = sorted(pivot.columns.tolist())
    pivot = pivot[month_columns] if month_columns else pivot
    pivot["total_responses"] = pivot.sum(axis=1)
    pivot.insert(0, "participant_uuid", pivot.index)
    pivot.insert(0, "participant_code", consent_meta["participant_code"].reindex(pivot.index).fillna(""))

    ordered_columns = ["participant_code", "participant_uuid", "total_responses"] + month_columns
    pivot = pivot[ordered_columns]

    return pivot.reset_index(drop=True)


def write_markdown_report(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("# Monthly Survey Participation\n\n")
        handle.write(
            "This table counts initial + biweekly survey submissions per participant,"
            " grouped by calendar month (YYYY-MM).\n\n"
        )
        if df.empty:
            handle.write("_No qualifying survey submissions were found._\n")
            return

        headers = df.columns.tolist()
        handle.write("| " + " | ".join(headers) + " |\n")
        handle.write("| " + " | ".join(["---"] * len(headers)) + " |\n")

        for _, row in df.iterrows():
            values = [_md_escape(row.get(col)) for col in headers]
            handle.write("| " + " | ".join(values) + " |\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarise survey submissions per calendar month")
    parser.add_argument(
        "--input",
        required=True,
        help="Directory containing structured CSVs",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Directory where the summary artefacts should be written",
    )
    parser.add_argument(
        "--latest-output",
        help="Optional CSV path that should always store the latest summary",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    structured_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = compute_monthly_counts(structured_dir)

    csv_path = output_dir / "monthly_participation_summary.csv"
    summary.to_csv(csv_path, index=False)

    if args.latest_output:
        latest_path = Path(args.latest_output).resolve()
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        summary.to_csv(latest_path, index=False)

    markdown_path = output_dir / "monthly_participation_summary.md"
    write_markdown_report(summary, markdown_path)

    print(f"📄 Wrote CSV summary to: {csv_path}")
    if args.latest_output:
        print(f"🔁 Updated rolling summary at: {Path(args.latest_output).resolve()}")
    print(f"📝 Wrote Markdown report to: {markdown_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
