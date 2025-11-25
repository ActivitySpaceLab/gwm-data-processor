# Pipeline Toolkit

This folder bundles everything a colleague needs to run the Gauteng Wellbeing Mapper data pipeline without touching Python tooling directly.

## Contents

- `pipeline_runner.py` – Orchestrates the download → decrypt → structure → report workflow.
- `requirements.txt` – Minimal dependency list used by the launcher.
- `.env.example` – Template for secrets. Copy to `.env` and populate values.
- `secrets/` – Drop `private_key.pem` here (ignored by git).
- `../run_pipeline.sh` – One-touch launcher (sits at the repository root and calls into this toolkit).

## Usage

1. Copy `.env.example` to `.env` and fill in:
   - `QUALTRICS_API_TOKEN`
   - `QUALTRICS_BASE_URL`
   - `QUALTRICS_SURVEY_ID`
   - Optional overrides for `PRIVATE_KEY_PASSWORD` and `PRIVATE_KEY_PATH`
     - If `PRIVATE_KEY_PASSWORD` is left blank or removed, you'll be prompted securely at runtime.
2. Place the RSA private key as `pipeline_toolkit/secrets/private_key.pem` or point `PRIVATE_KEY_PATH` to its location.
3. From the repository root, run the launcher:

```bash
./run_pipeline.sh
```

4. Structured outputs appear in `data/structured/<run-timestamp>/` and participant reports (plus embedded maps) appear in `data/reports/<run-timestamp>/` (each folder keeps a `latest/` symlink or `LATEST_RUN.txt` pointer for convenience). A JSON summary of the run is written to `pipeline_toolkit/last_run_summary.json`.

Images embedded in legacy survey payloads are ignored during structuring to keep the output tidy. If you ever need to recover them for forensic purposes, run `generate_survey_csvs.py` manually with `--download-images` after the pipeline finishes.

### Optional: ingest buffered S3 payloads

If the fallback proxy has stored encrypted submissions in S3, the pipeline can
fold them into the same run automatically. Configure the following environment
variables (typically in `.env`) before launching:

- `BUFFERED_S3_BUCKET` – required to enable the step; identifies the bucket.
- `BUFFERED_S3_PREFIX` – override the default `buffered-surveys/` prefix.
- `BUFFERED_S3_PROFILE` / `BUFFERED_S3_REGION` – optional AWS config.
- `BUFFERED_S3_MAX` – limit how many buffered objects are processed (useful for tests).
- `BUFFERED_S3_WRITE_RAW` / `BUFFERED_S3_WRITE_JSON` – set to `1`/`true` to keep per-object artefacts for debugging.

The downloader writes `buffered_survey_payloads.csv` into the run's `data/raw/`
directory. The decryption and structuring stages deduplicate payloads across
both Qualtrics and S3 sources using a stable hash of the encrypted data, so
running the fallback step does not introduce duplicate responses.

## Output folders

Every pipeline execution is stored separately to avoid clobbering earlier runs:

- `data/raw/<run-timestamp>/`
- `data/decrypted/<run-timestamp>/`
- `data/structured/<run-timestamp>/`
- `data/reports/<run-timestamp>/`

The toolkit also maintains a `latest/` symlink (or a `LATEST_RUN.txt` pointer on systems without symlink support) inside each folder so collaborators can always find the most recent results quickly.
