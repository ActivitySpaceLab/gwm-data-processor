#!/usr/bin/env python3
"""Convenient entrypoint for the Gauteng Wellbeing Mapper data pipeline.

This script orchestrates the four core phases required to turn encrypted
Qualtrics survey data into ready-to-analyse CSV tables and reports:

1. Download the latest survey responses from Qualtrics.
2. Decrypt the downloaded payloads using the team private key.
3. Structure the decrypted content into `biweekly_survey.csv`,
    `consent.csv`, `initial_survey.csv`, and `location_data.csv`.
4. Evaluate, per participant, how many of the 12 scheduled biweekly periods
    have at least one qualifying submission (for compensation tracking).
5. Summarise monthly participation counts (initial + biweekly submissions).
6. Generate per-participant HTML reports, including location maps.

All heavy lifting remains inside the specialised tool directories
(`qualtrics_tools`, `decryption_tools`, `structure_tools`).  The runner simply
chains them together, tracks progress, and leaves a concise JSON summary for
reference.

Typical usage for colleagues:

    ./run_pipeline.sh           # recommended wrapper (loads .env, venv, deps)
    python pipeline_runner.py    # direct invocation if you manage env manually

Environment variables (loaded automatically by ``run_pipeline.sh``):
    QUALTRICS_API_TOKEN    Required – Qualtrics API key with survey access.
    QUALTRICS_BASE_URL     Required – Qualtrics API host for your tenant.
    QUALTRICS_SURVEY_ID    Required – Survey ID for the unified responses.
    PRIVATE_KEY_PASSWORD   Required – password that unlocks the PEM key.
    PRIVATE_KEY_PATH       Optional – override path to PEM (default secrets/).

Outputs (created under ``data/``):
    data/raw/              CSV export downloaded from Qualtrics.
    data/decrypted/        Intermediate decrypted CSV files.
    data/structured/       Final CSVs + processing_report.json summary +
                          biweekly_period_summary.{csv,md} +
                          monthly_participation_summary.{csv,md}
    data/reports/          Participant HTML reports and embedded maps.
    pipeline_toolkit/last_run_summary.json   Metadata about the latest run.

The runner is intentionally chatty: progress is logged to stdout and any
sub-process stderr is surfaced immediately for troubleshooting.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from getpass import getpass
from pathlib import Path
from typing import Dict, List, Optional


def _ensure_utf8_output() -> None:
    """Align stdout/stderr encoding so Windows consoles accept UTF-8 output."""

    if os.name == "nt":
        try:
            import ctypes  # type: ignore

            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        except Exception:  # pragma: no cover - best-effort only
            pass

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_ensure_utf8_output()


@dataclass
class StepResult:
    """Outcome record for a pipeline stage."""

    name: str
    command: List[str]
    status: str
    started_at: str
    finished_at: str
    return_code: int
    stdout_tail: str
    stderr: str
    details: Dict[str, object]


class PipelineRunner:
    """High-level coordinator for the Qualtrics → CSV pipeline."""

    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = Path(project_root or Path(__file__).resolve().parent.parent)
        self.tools_dir = {
            "qualtrics": self.project_root / "qualtrics_tools",
            "decryption": self.project_root / "decryption_tools",
            "structure": self.project_root / "structure_tools",
        }
        self.buffer_download_script = self.tools_dir["qualtrics"] / "download_buffered_surveys.py"
        self.pipeline_dir = Path(__file__).resolve().parent
        self.data_root = self.project_root / "data"
        self.output_dirs = {
            "raw": self.data_root / "raw",
            "decrypted": self.data_root / "decrypted",
            "structured": self.data_root / "structured",
            "reports": self.data_root / "reports",
        }
        self.secrets_dir = self.pipeline_dir / "secrets"
        self.private_key_path = Path(
            os.environ.get("PRIVATE_KEY_PATH", self.secrets_dir / "private_key.pem")
        ).expanduser()
        self._step_results: List[StepResult] = []
        self._private_key_password: Optional[str] = os.environ.get("PRIVATE_KEY_PASSWORD")
        self.run_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.run_dirs = {
            label: path / self.run_timestamp for label, path in self.output_dirs.items()
        }

    # ------------------------------------------------------------------
    # Preparation and validation helpers
    # ------------------------------------------------------------------
    def prepare_environment(self, clean: bool = False) -> None:
        print("\n🔧 Preparing working directories...")
        if clean and self.data_root.exists():
            print(f"   • Removing existing data folder: {self.data_root}")
            shutil.rmtree(self.data_root)

        for label, path in self.output_dirs.items():
            path.mkdir(parents=True, exist_ok=True)
            run_path = self.run_dirs[label]
            run_path.mkdir(parents=True, exist_ok=True)
            print(f"   • Ready: {label} → {run_path}")

        self.secrets_dir.mkdir(parents=True, exist_ok=True)

    def validate_secrets(self) -> None:
        print("\n🔐 Validating credentials...")
        api_token = os.environ.get("QUALTRICS_API_TOKEN")
        if not api_token:
            raise ValueError(
                "QUALTRICS_API_TOKEN is missing. Copy .env.example to .env and supply your token."
            )

        base_url = os.environ.get("QUALTRICS_BASE_URL")
        if not base_url:
            raise ValueError(
                "QUALTRICS_BASE_URL is missing. Set it in .env so the downloader knows which Qualtrics host to use."
            )

        survey_id = os.environ.get("QUALTRICS_SURVEY_ID")
        if not survey_id:
            raise ValueError(
                "QUALTRICS_SURVEY_ID is missing. Set it in .env so the downloader targets the correct survey."
            )

        password = self._private_key_password
        if not password:
            password = getpass("Enter private key password: ")
            if not password:
                raise ValueError("Private key password is required to decrypt survey data.")
            self._private_key_password = password
        else:
            # Clear from inherited environment to reduce accidental leakage downstream
            os.environ.pop("PRIVATE_KEY_PASSWORD", None)

        if not self.private_key_path.exists():
            raise FileNotFoundError(
                f"Private key not found at {self.private_key_path}. Place your PEM inside secrets/."
            )

        print("   • API token detected (last 4: {} )".format(api_token[-4:]))
        print(f"   • Qualtrics base URL: {base_url}")
        print(f"   • Qualtrics survey ID: {survey_id}")
        print(f"   • Private key path: {self.private_key_path}")

    # ------------------------------------------------------------------
    # Step execution helpers
    # ------------------------------------------------------------------
    def run(self, days: Optional[int], all_data: bool, clean: bool) -> bool:
        start_time = datetime.now()
        print("\n🚀 Launching data pipeline run")
        print("=" * 60)

        self.prepare_environment(clean=clean)
        self.validate_secrets()

        success = True
        success &= self._download_step(days=days, all_data=all_data)
        success &= self._decrypt_step()
        success &= self._structure_step()
        success &= self._period_summary_step()
        success &= self._monthly_summary_step()
        success &= self._report_step()

        self._write_summary(start_time=start_time, success=success)
        return success

    def _download_step(self, days: Optional[int], all_data: bool) -> bool:
        print("\n📥 Step 1/6 – Downloading from Qualtrics...")
        cmd = [
            sys.executable,
            str(self.tools_dir["qualtrics"] / "download_qualtrics_data.py"),
            "--output",
            str(self.run_dirs["raw"]),
        ]

        if days:
            cmd += ["--days", str(days)]
        elif all_data:
            cmd.append("--all")

        success = self._run_command("download", cmd)

        if not success:
            return False

        bucket = os.environ.get("BUFFERED_S3_BUCKET")
        if not bucket or not self.buffer_download_script.exists():
            return success

        buffered_cmd = [
            sys.executable,
            str(self.buffer_download_script),
            "--bucket",
            bucket,
            "--output-dir",
            str(self.run_dirs["raw"]),
            "--csv-name",
            os.environ.get("BUFFERED_S3_CSV_NAME", "buffered_survey_payloads.csv"),
            "--skip-decrypt",
        ]

        prefix = os.environ.get("BUFFERED_S3_PREFIX")
        if prefix:
            buffered_cmd += ["--prefix", prefix]

        profile = os.environ.get("BUFFERED_S3_PROFILE")
        if profile:
            buffered_cmd += ["--profile", profile]

        region = os.environ.get("BUFFERED_S3_REGION")
        if region:
            buffered_cmd += ["--region", region]

        max_items = os.environ.get("BUFFERED_S3_MAX")
        if max_items:
            buffered_cmd += ["--max", max_items]

        if os.environ.get("BUFFERED_S3_WRITE_RAW", "0").lower() in {"1", "true", "yes"}:
            buffered_cmd.append("--write-raw")

        if os.environ.get("BUFFERED_S3_WRITE_JSON", "0").lower() in {"1", "true", "yes"}:
            buffered_cmd.append("--write-json")

        print("   • Detected BUFFERED_S3_BUCKET; downloading buffered payloads...")
        return success and self._run_command("buffered", buffered_cmd)

    def _decrypt_step(self) -> bool:
        print("\n🔓 Step 2/6 – Decrypting survey payloads...")
        cmd = [
            sys.executable,
            str(self.tools_dir["decryption"] / "automated_decryption_pipeline.py"),
            "--input",
            str(self.run_dirs["raw"]),
            "--output",
            str(self.run_dirs["decrypted"]),
            "--private-key",
            str(self.private_key_path),
        ]

        return self._run_command("decrypt", cmd)

    def _structure_step(self) -> bool:
        print("\n📊 Step 3/6 – Structuring CSV outputs...")
        cmd = [
            sys.executable,
            str(self.tools_dir["structure"] / "generate_survey_csvs.py"),
            "--input",
            str(self.run_dirs["decrypted"]),
            "--output",
            str(self.run_dirs["structured"]),
            "--report",
            "--validate",
        ]

        return self._run_command("structure", cmd)

    def _period_summary_step(self) -> bool:
        print("\n📆 Step 4/6 – Evaluating biweekly participation...")
        cmd = [
            sys.executable,
            str(self.tools_dir["structure"] / "calculate_biweekly_periods.py"),
            "--input",
            str(self.run_dirs["structured"]),
            "--output",
            str(self.run_dirs["structured"]),
            "--latest-output",
            str(self.output_dirs["structured"] / "biweekly_period_summary_latest.csv"),
        ]

        return self._run_command("periods", cmd)

    def _monthly_summary_step(self) -> bool:
        print("\n📈 Step 5/6 – Summarising monthly participation...")
        cmd = [
            sys.executable,
            str(self.tools_dir["structure"] / "calculate_monthly_participation.py"),
            "--input",
            str(self.run_dirs["structured"]),
            "--output",
            str(self.run_dirs["structured"]),
            "--latest-output",
            str(self.output_dirs["structured"] / "monthly_participation_summary_latest.csv"),
        ]

        return self._run_command("monthly", cmd)

    def _report_step(self) -> bool:
        print("\n🗺️ Step 6/6 – Building participant reports...")
        cmd = [
            sys.executable,
            str(self.tools_dir["structure"] / "generate_participant_reports.py"),
            "--input",
            str(self.run_dirs["structured"]),
            "--output",
            str(self.run_dirs["reports"]),
        ]

        return self._run_command("reports", cmd)

    def _run_command(self, name: str, cmd: List[str]) -> bool:
        started_at = datetime.now()
        print(f"   • Running: {' '.join(cmd)}")
        env = os.environ.copy()
        if self._private_key_password:
            env["PRIVATE_KEY_PASSWORD"] = self._private_key_password
        env["PIPELINE_RUN_TIMESTAMP"] = self.run_timestamp
        env["PIPELINE_OUTPUT_DIR"] = str(self.run_dirs["structured"])
        env["PIPELINE_REPORT_DIR"] = str(self.run_dirs["reports"])
        env.setdefault("PYTHONUTF8", "1")

        result = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )

        finished_at = datetime.now()
        stdout_tail = (result.stdout or "").strip()[-2000:]
        stderr = (result.stderr or "").strip()
        status = "success" if result.returncode == 0 else "failed"

        if stdout_tail:
            print("   • stdout (tail):")
            for line in stdout_tail.splitlines()[-10:]:
                print(f"     {line}")
        if stderr:
            print("   • stderr:")
            for line in stderr.splitlines():
                print(f"     {line}")

        step_result = StepResult(
            name=name,
            command=cmd,
            status=status,
            started_at=started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            return_code=result.returncode,
            stdout_tail=stdout_tail,
            stderr=stderr,
            details={
                "working_directory": str(self.project_root),
            },
        )
        self._step_results.append(step_result)

        if result.returncode == 0:
            print(f"   ✅ {name.capitalize()} step completed")
            return True

        print(f"   ❌ {name.capitalize()} step failed (exit={result.returncode})")
        return False

    # ------------------------------------------------------------------
    # Reporting helpers
    # ------------------------------------------------------------------
    def _write_summary(self, start_time: datetime, success: bool) -> None:
        summary_path = self.pipeline_dir / "last_run_summary.json"
        structured_dir = self.run_dirs["structured"]

        payload = {
            "started_at": start_time.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "success": success,
            "data_root": str(self.data_root),
            "run_timestamp": self.run_timestamp,
            "output_directories": {
                label: str(path) for label, path in self.run_dirs.items()
            },
            "structured_output": str(structured_dir),
            "reports_output": str(self.run_dirs["reports"]),
            "steps": [asdict(step) for step in self._step_results],
        }

        summary_path.write_text(json.dumps(payload, indent=2))
        print("\n📝 Summary written to", summary_path)
        self._refresh_latest_markers()
        if success:
            print("🎉 Pipeline finished successfully!")
            print(f"   Structured CSVs live in: {structured_dir}")
            print(f"   Participant reports live in: {self.run_dirs['reports']}")
        else:
            print("💥 Pipeline finished with errors. Review the summary for details.")

    def _refresh_latest_markers(self) -> None:
        for label, base_dir in self.output_dirs.items():
            run_dir = self.run_dirs[label]
            symlink_path = base_dir / "latest"
            marker_file = base_dir / "LATEST_RUN.txt"

            try:
                if symlink_path.exists() or symlink_path.is_symlink():
                    if symlink_path.is_dir() and not symlink_path.is_symlink():
                        shutil.rmtree(symlink_path)
                    else:
                        symlink_path.unlink()
                symlink_path.symlink_to(run_dir, target_is_directory=True)
                if marker_file.exists():
                    marker_file.unlink()
            except OSError as exc:  # noqa: PERF203
                marker_file.write_text(
                    f"Latest run directory: {run_dir}\n(Symlink creation failed: {exc})\n"
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download, decrypt, and structure Gauteng Wellbeing Mapper survey data.",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Only download the last N days of data (default: all available data).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Force a full download of all available data (default when --days is not provided).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing data/ folders before running (fresh start).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner = PipelineRunner()
    try:
        days = args.days if args.days and args.days > 0 else None
        all_data = args.all or not days
        success = runner.run(days=days, all_data=all_data, clean=args.clean)
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⚠️ Run cancelled by user")
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"\n💥 Pipeline aborted: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
