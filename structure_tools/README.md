# Structure Tools

Tools for structuring the decrypted Qualtrics payloads, generating reports, and analysing output quality.

## Files

- **`generate_survey_csvs.py`** – Primary entrypoint used by the pipeline to build the structured CSV outputs
- **`calculate_biweekly_periods.py`** – Counts, per participant, how many of the scheduled 12 biweekly periods include at least one qualifying submission
- **`generate_participant_reports.py`** – Builds per-participant HTML summaries with optional location maps
- **`create_structured_tables.py`** – Legacy helper for converting decrypted data to analysis-ready formats
- **`analyze_encryption_limits.py`** - Analyze encryption performance and size limits
- **`realistic_location_analysis.py`** - Tools for analyzing location data patterns
- **`encryption_limits_report.md`** - Detailed analysis report on encryption limits

## Quick Start

1. Create structured tables from decrypted data (mostly superseded by `generate_survey_csvs.py`):
   ```bash
   python create_structured_tables.py
   ```

2. Generate structured CSVs and validation report (used by the pipeline):
   ```bash
   python generate_survey_csvs.py --input ../data/decrypted/latest --output ../data/structured/latest --validate
   ```

3. Summarise biweekly participation/compliance:
   ```bash
   python calculate_biweekly_periods.py --input ../data/structured/latest --output ../data/structured/latest
   ```

4. Produce per-participant HTML reports:
   ```bash
   python generate_participant_reports.py --input ../data/structured/latest --output ../data/reports/latest
   ```

5. Analyze encryption performance:
   ```bash
   python analyze_encryption_limits.py
   ```

6. Analyze location data:
   ```bash
   python realistic_location_analysis.py
   ```
