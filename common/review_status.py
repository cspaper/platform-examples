from typing import Optional

TERMINAL_SUCCESS_STATUSES = {"COMPLETED"}
TERMINAL_FAILURE_STATUSES = {"FAILED"}
PENDING_STATUSES = {"PENDING", "PROCESSING"}
KNOWN_STATUSES = (
    TERMINAL_SUCCESS_STATUSES | TERMINAL_FAILURE_STATUSES | PENDING_STATUSES
)


def normalize_status(status: Optional[str]) -> str:
    return str(status or "").strip().upper()


def is_terminal_failure(status: Optional[str], failed_reason: Optional[str] = None) -> bool:
    del failed_reason
    return normalize_status(status) in TERMINAL_FAILURE_STATUSES


def is_pending(status: Optional[str]) -> bool:
    return normalize_status(status) in PENDING_STATUSES


def is_known_status(status: Optional[str]) -> bool:
    return normalize_status(status) in KNOWN_STATUSES


def is_terminal(status: Optional[str], failed_reason: Optional[str] = None) -> bool:
    normalized = normalize_status(status)
    return normalized in TERMINAL_SUCCESS_STATUSES or is_terminal_failure(
        normalized, failed_reason
    )
