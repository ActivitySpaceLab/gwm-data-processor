#!/usr/bin/env python3
"""Download buffered survey payloads from S3 for inclusion in the pipeline.

The proxy service can write encrypted survey submissions into an S3 bucket when
Qualtrics rejects requests or is temporarily offline.  This script retrieves
those buffered payloads, stores a CSV that mirrors the Qualtrics export layout
(at minimum the ``survey_type`` and ``encrypted_data`` columns), and
optionally performs local decryption for inspection.

Key capabilities:
	* Traverses a ``buffered-surveys/<survey_type>/`` prefix hierarchy.
	* Writes `buffered_survey_payloads.csv` with one row per unique payload.
	* Adds an ``encrypted_data_hash`` column so downstream stages can dedupe
	  across both the S3 buffer and Qualtrics proper.
	* (Optional) decrypts payloads using the existing toolkit helper for
	  ad-hoc debugging.

Typical pipeline usage (raw download only):
	python download_buffered_surveys.py \
		--bucket gwm-survey-fallback-afsouth1 \
		--output-dir data/raw/current_run \
		--skip-decrypt

Manual inspection with decryption:
	python download_buffered_surveys.py \
		--bucket gwm-survey-fallback-afsouth1 \
		--prefix buffered-surveys/initial/ \
		--private-key ../pipeline_toolkit/secrets/private_key.pem

AWS credentials must permit ``s3:ListBucket`` and ``s3:GetObject`` for the
target bucket.  Configure them via environment variables, credential files, or
named profiles before executing this script.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import pathlib
import sys
from dataclasses import dataclass
from datetime import datetime
from getpass import getpass
from typing import Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import ClientError

DEFAULT_PREFIX = "buffered-surveys/"
DEFAULT_CSV_NAME = "buffered_survey_payloads.csv"


@dataclass
class BufferedPayload:
	"""Representation of a buffered survey payload."""

	bucket: str
	key: str
	survey_type: str
	payload: str
	last_modified: Optional[datetime]
	etag: Optional[str]

	@property
	def encrypted_hash(self) -> str:
		return hashlib.sha256(self.payload.encode("utf-8")).hexdigest()

	@property
	def response_id(self) -> str:
		# Stable identifier; aligning duplicates with identical payloads.
		return f"s3::{self.encrypted_hash}"

	def to_row(self) -> Dict[str, str]:
		return {
			"ResponseId": self.response_id,
			"survey_type": self.survey_type,
			"encrypted_data": self.payload,
			"encrypted_data_hash": self.encrypted_hash,
			"buffer_source": "s3",
			"buffer_bucket": self.bucket,
			"buffer_key": self.key,
			"buffer_last_modified": self.last_modified.isoformat() if self.last_modified else "",
			"buffer_etag": (self.etag or "").strip('"'),
		}


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Download buffered S3 survey payloads and prepare pipeline input"
	)
	parser.add_argument("--bucket", required=True, help="S3 bucket name containing buffered payloads")
	parser.add_argument(
		"--prefix",
		default=DEFAULT_PREFIX,
		help="S3 key prefix to scan (default: buffered-surveys/)",
	)
	parser.add_argument(
		"--output-dir",
		default="buffered_downloads",
		help="Directory for CSV + optional raw/decrypted artefacts",
	)
	parser.add_argument(
		"--csv-name",
		default=DEFAULT_CSV_NAME,
		help="Filename for the aggregated CSV inside --output-dir",
	)
	parser.add_argument("--max", type=int, default=None, help="Optional cap on number of S3 objects to process")
	parser.add_argument("--profile", default=None, help="Optional AWS profile name")
	parser.add_argument("--region", default=None, help="Override AWS region for the S3 client")
	parser.add_argument(
		"--skip-decrypt",
		action="store_true",
		help="Download payloads without decrypting (recommended for pipeline runs)",
	)
	parser.add_argument(
		"--private-key",
		default="../pipeline_toolkit/secrets/private_key.pem",
		help="Path to the PEM-encoded RSA private key (required if not skipping decryption)",
	)
	parser.add_argument(
		"--passphrase",
		default=None,
		help="Optional private key passphrase (omit to prompt interactively)",
	)
	parser.add_argument(
		"--write-raw",
		action="store_true",
		help="Persist each raw payload as a text file under output-dir/raw/",
	)
	parser.add_argument(
		"--write-json",
		action="store_true",
		help="When decrypting, persist decrypted JSON under output-dir/decrypted/",
	)
	return parser.parse_args()


def make_s3_client(profile: Optional[str], region: Optional[str]):
	session_kwargs = {"profile_name": profile} if profile else {}
	session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
	return session.client("s3", region_name=region)


def ensure_output_dirs(base_dir: pathlib.Path, write_raw: bool, write_json: bool) -> Dict[str, pathlib.Path]:
	base_dir.mkdir(parents=True, exist_ok=True)
	paths = {"base": base_dir}
	if write_raw:
		paths["raw"] = base_dir / "raw"
		paths["raw"].mkdir(parents=True, exist_ok=True)
	if write_json:
		paths["decrypted"] = base_dir / "decrypted"
		paths["decrypted"].mkdir(parents=True, exist_ok=True)
	return paths


def list_keys(client, bucket: str, prefix: str) -> Iterable[str]:
	paginator = client.get_paginator("list_objects_v2")
	for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
		for item in page.get("Contents", []):
			key = item["Key"]
			if key.endswith("/"):
				continue
			yield key


def infer_survey_type(key: str, prefix: str) -> str:
	trimmed = key[len(prefix) :] if key.startswith(prefix) else key
	parts = [segment for segment in trimmed.split("/") if segment]
	return (parts[0] if parts else "unknown").lower()


def download_payload(client, bucket: str, key: str, prefix: str) -> Optional[BufferedPayload]:
	try:
		response = client.get_object(Bucket=bucket, Key=key)
	except ClientError as err:
		print(f"   ❌ Failed to download {key}: {err}")
		return None

	body = response["Body"].read()
	try:
		payload = body.decode("utf-8")
	except UnicodeDecodeError as exc:
		print(f"   ❌ Could not decode payload for {key}: {exc}")
		return None

	return BufferedPayload(
		bucket=bucket,
		key=key,
		survey_type=infer_survey_type(key, prefix),
		payload=payload,
		last_modified=response.get("LastModified"),
		etag=response.get("ETag"),
	)


def write_records_csv(records: List[BufferedPayload], csv_path: pathlib.Path) -> None:
	csv_path.parent.mkdir(parents=True, exist_ok=True)
	fieldnames = sorted(records[0].to_row().keys()) if records else [
		"ResponseId",
		"survey_type",
		"encrypted_data",
		"encrypted_data_hash",
		"buffer_source",
		"buffer_bucket",
		"buffer_key",
		"buffer_last_modified",
		"buffer_etag",
	]

	with csv_path.open("w", encoding="utf-8", newline="") as handle:
		writer = csv.DictWriter(handle, fieldnames=fieldnames)
		writer.writeheader()
		for record in records:
			writer.writerow(record.to_row())


def maybe_decrypt_payload(
	record: BufferedPayload,
	private_key: pathlib.Path,
	passphrase: Optional[str],
	decrypted_dir: Optional[pathlib.Path],
) -> bool:
	try:
		from decryption_tools.decrypt_survey_data import decrypt_data
	except ImportError as exc:  # pragma: no cover - defensive guard
		print(f"   ❌ decrypt_survey_data import failed: {exc}")
		return False

	decrypted = decrypt_data(record.payload, str(private_key), passphrase or "")
	if not decrypted:
		print("   ⚠️ Decryption failed")
		return False

	if decrypted_dir:
		safe_key = record.key.strip("/").replace("/", "__")
		target = decrypted_dir / f"{safe_key}.json"
		target.write_text(json.dumps(decrypted, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
		print(f"   💾 Wrote decrypted JSON to {target}")
	else:
		print(json.dumps(decrypted, indent=2, ensure_ascii=False))

	return True


def save_raw_payload(record: BufferedPayload, raw_dir: Optional[pathlib.Path]) -> None:
	if not raw_dir:
		return

	safe_key = record.key.strip("/").replace("/", "__")
	target = raw_dir / f"{safe_key}.txt"
	target.write_text(record.payload, encoding="utf-8")


def main() -> int:
	args = parse_args()

	try:
		s3_client = make_s3_client(args.profile, args.region)
	except Exception as exc:  # pragma: no cover - boto3 raises varied errors
		print(f"❌ Failed to configure S3 client: {exc}", file=sys.stderr)
		return 1

	output_paths = ensure_output_dirs(
		pathlib.Path(args.output_dir).resolve(), write_raw=args.write_raw, write_json=args.write_json
	)
	csv_path = output_paths["base"] / args.csv_name

	passphrase: Optional[str]
	if args.skip_decrypt:
		passphrase = None
	else:
		passphrase = args.passphrase or getpass("Enter private key passphrase (press Enter if none): ") or None

	seen_hashes: set[str] = set()
	records: List[BufferedPayload] = []
	processed = 0
	duplicates = 0
	decrypted_ok = 0
	decrypted_fail = 0

	print(f"🔍 Scanning s3://{args.bucket}/{args.prefix}")

	for key in list_keys(s3_client, args.bucket, args.prefix):
		if args.max is not None and processed >= args.max:
			break

		processed += 1
		print(f"\n📦 Processing {key}")

		record = download_payload(s3_client, args.bucket, key, args.prefix)
		if record is None:
			continue

		payload_hash = record.encrypted_hash
		if payload_hash in seen_hashes:
			duplicates += 1
			print("   🔁 Duplicate payload detected; skipping")
			continue

		seen_hashes.add(payload_hash)
		save_raw_payload(record, output_paths.get("raw"))
		records.append(record)

		if not args.skip_decrypt:
			private_key_path = pathlib.Path(args.private_key).expanduser().resolve()
			ok = maybe_decrypt_payload(record, private_key_path, passphrase, output_paths.get("decrypted"))
			decrypted_ok += 1 if ok else 0
			decrypted_fail += 0 if ok else 1

	write_records_csv(records, csv_path)

	print("\n=== Buffered Survey Download Summary ===")
	print(f"Bucket: {args.bucket}")
	print(f"Prefix: {args.prefix}")
	print(f"Objects inspected: {processed}")
	print(f"Unique payloads written: {len(records)}")
	print(f"Duplicates skipped: {duplicates}")
	print(f"CSV output: {csv_path}")
	if args.write_raw:
		print(f"Raw payloads stored in: {output_paths['raw']}")
	if not args.skip_decrypt:
		print(f"Successful decryptions: {decrypted_ok}")
		print(f"Failed decryptions: {decrypted_fail}")
		if args.write_json:
			print(f"Decrypted JSON stored in: {output_paths['decrypted']}")

	return 0


if __name__ == "__main__":
	sys.exit(main())
