#!/usr/bin/env python3
"""Compatibility wrapper for the relocated buffered survey downloader."""

from __future__ import annotations

import sys
import warnings


def main() -> int:
    warnings.warn(
        "download_and_decrypt_buffered_surveys.py has moved to"
        " qualtrics_tools/download_buffered_surveys.py; please update invocations.",
        DeprecationWarning,
        stacklevel=2,
    )
    from qualtrics_tools.download_buffered_surveys import main as delegate

    return delegate()


if __name__ == "__main__":
    sys.exit(main())
