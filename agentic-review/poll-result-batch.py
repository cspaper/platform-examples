"""
Poll partner review jobs from a submissions.json file and save results as .md files.
Idempotent: skips jobs whose .md already exists. Safe to re-run on partial batches.

Reads from a sibling output/submissions.json and writes .md files to the same output/ directory.

Requires: python -m pip install -r requirements.txt
Reads API_KEY and API_URL from .env.

Usage:
    python poll-result-batch.py \
        [--paper-dir ./papers] \
        [--poll-interval 30] \
        [--timeout 1800]
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import httpx
from common.env_utils import resolve_api_key, resolve_api_url


def load_submissions(path: Path) -> List[dict]:
    if not path.exists():
        print(f"Error: {path} not found", file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text())


def fetch_review(client: httpx.Client, api_key: str, job_id: str) -> Optional[dict]:
    try:
        response = client.get(
            f"/api/platform/reviews/{job_id}",
            headers={"X-API-Key": api_key},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["data"]
    except Exception as exc:
        print(f"  WARN fetch {job_id}: {exc}", file=sys.stderr)
        return None


def result_path(output_dir: Path, entry: dict) -> Path:
    stem = Path(entry["filename"]).stem
    agent_id = entry.get("agent_id")
    if not agent_id:
        return output_dir / f"{stem}.md"

    safe_agent_id = re.sub(r"[^A-Za-z0-9._-]+", "_", agent_id)
    return output_dir / f"{stem}__{safe_agent_id}.md"


def save_md(output_dir: Path, entry: dict, review: dict) -> Path:
    out_path = result_path(output_dir, entry)

    paper_meta = review.get("paper_meta") or {}
    result_summary = review.get("result_summary") or {}
    title = paper_meta.get("title") or entry["filename"]
    main_score_norm = result_summary.get("mainScoreNorm")
    desk_reject = result_summary.get("deskReject")

    frontmatter_lines = [
        "---",
        f"job_id: {review['id']}",
        f"agent_id: {entry['agent_id']}",
        f"status: {review['status']}",
        f"filename: {entry['filename']}",
        f"paper: {title}",
        f"main_score_norm: {main_score_norm if main_score_norm is not None else 'N/A'}",
        f"desk_reject: {str(desk_reject).lower() if desk_reject is not None else 'N/A'}",
        "---",
        "",
    ]

    result_text = review.get("result") or ""
    if review["status"] == "FAILED":
        result_text = f"FAILED: {review.get('failed_reason', 'unknown')}"

    content = "\n".join(frontmatter_lines) + result_text
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return out_path


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

    paper_dir = Path(args.paper_dir)
    output_dir = paper_dir.parent / "output"
    submissions_path = output_dir / "submissions.json"
    submissions = load_submissions(submissions_path)

    # Build pending list — skip already-saved results
    pending: Dict[str, dict] = {}  # job_id -> submission entry
    for entry in submissions:
        out_path = result_path(output_dir, entry)
        if out_path.exists():
            print(
                f"  skip {entry['filename']} for agent {entry.get('agent_id', 'unknown')} "
                f"(already collected at {out_path.name})"
            )
        else:
            pending[entry["job_id"]] = entry

    if not pending:
        print("All reviews already collected.")
        return

    print(f"\nPolling {len(pending)} job(s) — interval {args.poll_interval}s, timeout {args.timeout}s\n")

    started_at = time.monotonic()
    timed_out: List[str] = []

    with httpx.Client(base_url=api_url) as client:
        while pending:
            if time.monotonic() - started_at > args.timeout:
                timed_out = list(pending.keys())
                break

            still_pending: Dict[str, dict] = {}
            for job_id, entry in pending.items():
                review = fetch_review(client, api_key, job_id)
                if review is None:
                    still_pending[job_id] = entry
                    continue

                status = review.get("status", "")
                if status in ("COMPLETED", "FAILED"):
                    out_path = save_md(output_dir, entry, review)
                    print(f"  [{status}] {entry['filename']} -> {out_path}")
                else:
                    still_pending[job_id] = entry

            pending = still_pending
            if pending:
                print(f"  {len(pending)} job(s) still pending — waiting {args.poll_interval}s...")
                time.sleep(args.poll_interval)

    if timed_out:
        print(f"\nTimeout reached. {len(timed_out)} job(s) did not complete:")
        for job_id in timed_out:
            print(f"  {pending[job_id]['filename']} ({job_id})")
        sys.exit(1)
    else:
        print("\nAll jobs collected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect partner review results from a sibling output/submissions.json and save as .md files."
    )
    parser.add_argument("--paper-dir", default="./papers", help="Path to folder containing paper PDF files (default: ./papers)")
    parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between poll passes (default: 30)")
    parser.add_argument("--timeout", type=int, default=1800, help="Max seconds to wait total (default: 1800)")

    main(parser.parse_args())
