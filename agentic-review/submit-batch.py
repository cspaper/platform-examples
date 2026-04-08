"""
Sequentially submit all PDFs in a folder to POST /api/v1/platform/review.
Saves job IDs to a sibling output/submissions.json for later collection
via poll-result-batch.py, which also writes .md results there.

Requires: python -m pip install -r requirements.txt
Reads API_KEY and API_URL from .env.

Usage:
    python submit-batch.py \
        --agent-id "AAAI_main technical_2026_1" \
        [--paper-dir ./papers]

The current full list of available agent IDs is on:
    https://cspaper.org/platform/review
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import httpx
from common.env_utils import resolve_api_key, resolve_api_url
from common.review_status import is_terminal_failure, normalize_status


def submission_key(filename: str, agent_id: str) -> str:
    return f"{filename}::{agent_id}"


def load_submissions(out_path: Path) -> Dict[str, dict]:
    """Return existing submissions keyed by filename and agent ID."""
    if out_path.exists():
        entries = json.loads(out_path.read_text())
        return {
            submission_key(e["filename"], e.get("agent_id", "")): e
            for e in entries
        }
    return {}


def save_submissions(out_path: Path, submissions: Dict[str, dict]) -> None:
    out_path.write_text(json.dumps(list(submissions.values()), indent=2))


def is_failed_submission(entry: dict) -> bool:
    return is_terminal_failure(entry.get("status"), entry.get("failed_reason"))


def submit_job(
    client: httpx.Client, api_key: str, pdf_path: Path, agent_id: str
) -> Optional[dict]:
    try:
        with pdf_path.open("rb") as f:
            response = client.post(
                "/api/platform/review",
                headers={"X-API-Key": api_key},
                data={"agent_id": agent_id},
                files={"file": (pdf_path.name, f, "application/pdf")},
                timeout=60,
            )
        response.raise_for_status()
        job_id = response.json()["data"]["job_id"]
        print(f"  submitted {pdf_path.name} -> job {job_id}")
        return {
            "filename": pdf_path.name,
            "job_id": job_id,
            "agent_id": agent_id,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        print(f"  ERROR {pdf_path.name}: {exc}", file=sys.stderr)
        return None


def main(args: argparse.Namespace) -> None:
    api_key = resolve_api_key()
    if not api_key:
        print(
            "Error: API key not found. Set API_KEY in .env.",
            file=sys.stderr,
        )
        sys.exit(1)

    api_url = resolve_api_url()
    if not api_url:
        print(
            "Error: API URL not found. Set API_URL in .env.",
            file=sys.stderr,
        )
        sys.exit(1)

    folder = Path(args.paper_dir)
    if not folder.is_dir():
        print(f"Error: {folder} is not a directory", file=sys.stderr)
        sys.exit(1)

    pdfs = sorted(folder.glob("*.pdf"))
    if not pdfs:
        print(f"No PDF files found in {folder}")
        return

    output_dir = folder.parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "submissions.json"
    submissions = load_submissions(out_path)

    skipped = []
    retries = []
    to_submit = []
    for pdf in pdfs:
        key = submission_key(pdf.name, args.agent_id)
        existing = submissions.get(key)
        if existing and is_failed_submission(existing):
            retries.append((pdf, existing))
            to_submit.append(pdf)
        elif existing:
            skipped.append((pdf, existing))
        else:
            to_submit.append(pdf)

    print(
        f"Found {len(pdfs)} PDF(s): {len(to_submit)} to submit "
        f"({len(retries)} retrying failed), {len(skipped)} already recorded"
    )
    for pdf, existing in retries:
        print(
            f"  retry {pdf.name} for agent {args.agent_id} "
            f"(previous job {existing.get('job_id', 'unknown')} failed with "
            f"status {normalize_status(existing.get('status')) or 'UNKNOWN'})"
        )
    for pdf, existing in skipped:
        print(
            f"  skip {pdf.name} for agent {args.agent_id} "
            f"(already submitted as job {existing.get('job_id', 'unknown')})"
        )

    submitted = 0
    errors = 0
    with httpx.Client(base_url=api_url) as client:
        for pdf in to_submit:
            entry = submit_job(client, api_key, pdf, args.agent_id)
            if entry:
                submissions[submission_key(pdf.name, args.agent_id)] = entry
                save_submissions(out_path, submissions)
                submitted += 1
            else:
                errors += 1

    print(f"\n{len(to_submit)} submitted: {submitted} ok, {errors} failed")
    print(f"Submissions saved to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Submit PDFs to the partner review API and save job IDs."
    )
    parser.add_argument("--paper-dir", default="./papers", help="Path to folder containing paper PDF files (default: ./papers)")
    # Current full list of available agent IDs: https://cspaper.org/platform/review
    parser.add_argument("--agent-id", required=True, help="Conference template ID (e.g. AAAI_main_technical_2026_1; see https://cspaper.org/platform/review)")

    main(parser.parse_args())
