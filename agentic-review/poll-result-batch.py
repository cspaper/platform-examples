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
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import httpx
from common.env_utils import resolve_api_key, resolve_api_url
from common.review_status import (
    KNOWN_STATUSES,
    PENDING_STATUSES,
    TERMINAL_SUCCESS_STATUSES,
    is_terminal,
    is_terminal_failure,
    normalize_status,
)


def load_submissions(path: Path) -> List[dict]:
    if not path.exists():
        print(f"Error: {path} not found", file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text())


def save_submissions(path: Path, submissions: List[dict]) -> None:
    path.write_text(json.dumps(submissions, indent=2) + "\n", encoding="utf-8")


def build_failed_review(job_id: str, reason: str, status: str = "FAILED") -> dict:
    return {
        "id": job_id,
        "status": status,
        "failed_reason": reason,
        "result": "",
    }


def fetch_review(client: httpx.Client, api_key: str, job_id: str) -> Optional[dict]:
    try:
        response = client.get(
            f"/api/platform/reviews/{job_id}",
            headers={"X-API-Key": api_key},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["data"]
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        if status_code in (401, 403):
            raise RuntimeError(
                f"Authentication failed while fetching job {job_id} "
                f"(HTTP {status_code}). Check API_KEY and API_URL."
            ) from exc
        if status_code in (404, 410):
            reason = f"Job lookup returned HTTP {status_code}; treating as terminal failure."
            print(f"  WARN fetch {job_id}: {reason}", file=sys.stderr)
            return build_failed_review(job_id, reason)

        print(f"  WARN fetch {job_id}: {exc}", file=sys.stderr)
        return None
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


def stored_result_path(out_path: Path) -> str:
    try:
        return str(out_path.relative_to(REPO_ROOT))
    except ValueError:
        return str(out_path)


def parse_frontmatter(md_path: Path) -> Dict[str, str]:
    lines = md_path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 3 or lines[0] != "---":
        return {}

    meta: Dict[str, str] = {}
    for line in lines[1:]:
        if line == "---":
            break
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        meta[key] = value
    return meta


def saved_result_matches_submission(entry: dict, out_path: Path) -> bool:
    meta = parse_frontmatter(out_path)
    saved_job_id = str(meta.get("job_id") or "").strip()
    current_job_id = str(entry.get("job_id") or "").strip()
    return bool(saved_job_id and current_job_id and saved_job_id == current_job_id)


def update_submission(entry: dict, review: dict, out_path: Path) -> bool:
    status = normalize_status(review.get("status"))
    changed = False
    updates = {
        "status": status,
        "polled_at": datetime.now(timezone.utc).isoformat(),
    }
    if status in TERMINAL_SUCCESS_STATUSES:
        updates["result_path"] = stored_result_path(out_path)
    elif "result_path" in entry:
        updates["result_path"] = None

    if is_terminal(review.get("status"), review.get("failed_reason")):
        updates["resolved_at"] = datetime.now(timezone.utc).isoformat()

    failed_reason = review.get("failed_reason")
    if is_terminal_failure(review.get("status"), failed_reason) and not failed_reason:
        failed_reason = "unknown"

    if failed_reason:
        updates["failed_reason"] = failed_reason
    elif status in TERMINAL_SUCCESS_STATUSES and "failed_reason" in entry:
        updates["failed_reason"] = None

    for key, value in updates.items():
        if entry.get(key) != value:
            entry[key] = value
            changed = True

    if entry.get("result_path") is None:
        entry.pop("result_path", None)
    if entry.get("failed_reason") is None:
        entry.pop("failed_reason", None)

    return changed


def update_submission_from_saved_markdown(entry: dict, out_path: Path) -> bool:
    meta = parse_frontmatter(out_path)
    if not meta:
        return False

    review = {
        "status": meta.get("status", ""),
        "failed_reason": meta.get("failed_reason"),
    }
    if "job_id" in meta:
        review["id"] = meta["job_id"]
    return update_submission(entry, review, out_path)


def resolved_summary(submissions: List[dict]) -> str:
    completed = 0
    failed = 0
    other = 0

    for entry in submissions:
        status = normalize_status(entry.get("status"))
        if status in TERMINAL_SUCCESS_STATUSES:
            completed += 1
        elif status and is_terminal_failure(status, entry.get("failed_reason")):
            failed += 1
        else:
            other += 1

    parts = [f"{completed} completed", f"{failed} failed"]
    if other:
        parts.append(f"{other} unresolved")
    return ", ".join(parts)


def save_md(output_dir: Path, entry: dict, review: dict) -> Path:
    out_path = result_path(output_dir, entry)

    paper_meta = review.get("paper_meta") or {}
    result_summary = review.get("result_summary") or {}
    title = paper_meta.get("title") or entry["filename"]
    main_score_norm = result_summary.get("mainScoreNorm")
    desk_reject = result_summary.get("deskReject")
    failed_reason = review.get("failed_reason")
    if is_terminal_failure(review.get("status"), failed_reason) and not failed_reason:
        failed_reason = "unknown"

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
    if failed_reason:
        frontmatter_lines.insert(-2, f"failed_reason: {failed_reason}")

    result_text = review.get("result") or ""
    if is_terminal_failure(review.get("status"), failed_reason):
        result_text = f"{normalize_status(review.get('status')) or 'FAILED'}: {failed_reason or 'unknown'}"

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
    submissions_changed = False
    for entry in submissions:
        out_path = result_path(output_dir, entry)
        if out_path.exists():
            if saved_result_matches_submission(entry, out_path):
                if update_submission_from_saved_markdown(entry, out_path):
                    submissions_changed = True
                print(
                    f"  skip {entry['filename']} for agent {entry.get('agent_id', 'unknown')} "
                    f"(already collected at {out_path.name})"
                )
            else:
                print(
                    f"  stale saved result for {entry['filename']} at {out_path.name} "
                    f"does not match current job {entry['job_id']} and will be overwritten"
                )
                pending[entry["job_id"]] = entry
        else:
            pending[entry["job_id"]] = entry

    if submissions_changed:
        save_submissions(submissions_path, submissions)

    if not pending:
        print(f"No pending reviews. {resolved_summary(submissions)}.")
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
                try:
                    review = fetch_review(client, api_key, job_id)
                except RuntimeError as exc:
                    print(f"Error: {exc}", file=sys.stderr)
                    sys.exit(1)
                if review is None:
                    still_pending[job_id] = entry
                    continue

                status = normalize_status(review.get("status"))
                if is_terminal(review.get("status"), review.get("failed_reason")):
                    out_path = save_md(output_dir, entry, review)
                    update_submission(entry, review, out_path)
                    save_submissions(submissions_path, submissions)
                    print(f"  [{status}] {entry['filename']} -> {out_path}")
                else:
                    entry_changed = False
                    polled_at = datetime.now(timezone.utc).isoformat()
                    if entry.get("status") != status:
                        entry["status"] = status
                        entry_changed = True
                    if entry.get("polled_at") != polled_at:
                        entry["polled_at"] = polled_at
                        entry_changed = True
                    if entry_changed:
                        save_submissions(submissions_path, submissions)
                    if status and status not in KNOWN_STATUSES:
                        print(
                            f"  WARN unrecognized job status for {job_id}: {status}",
                            file=sys.stderr,
                        )
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
        print(f"\nNo pending reviews. {resolved_summary(submissions)}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect partner review results from a sibling output/submissions.json and save as .md files."
    )
    parser.add_argument("--paper-dir", default="./papers", help="Path to folder containing paper PDF files (default: ./papers)")
    parser.add_argument("--poll-interval", type=int, default=30, help="Seconds between poll passes (default: 30)")
    parser.add_argument("--timeout", type=int, default=1800, help="Max seconds to wait total (default: 1800)")

    main(parser.parse_args())
