import os
from pathlib import Path
from typing import List, Optional, Set


def load_dotenv() -> None:
    """Load simple KEY=VALUE pairs from the nearest supported .env file."""
    for env_path in _candidate_env_paths():
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            os.environ.setdefault(key, value)
        return


def resolve_api_key() -> Optional[str]:
    load_dotenv()
    return os.getenv("API_KEY")


def resolve_api_url() -> Optional[str]:
    load_dotenv()
    return os.getenv("API_URL")


def _candidate_env_paths() -> List[Path]:
    module_dir = Path(__file__).resolve().parent
    repo_root = module_dir.parent
    cwd = Path.cwd().resolve()
    candidates = [
        cwd / ".env",
        repo_root / ".env",
        module_dir / ".env",
    ]

    unique_candidates: List[Path] = []
    seen: Set[Path] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        unique_candidates.append(path)
    return unique_candidates
