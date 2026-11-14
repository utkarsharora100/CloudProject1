import os
from pathlib import Path


def require_local_emulator():
    if os.environ.get("APP_ENV") != "local" or os.environ.get("PROJECT_ID") != "demo-food-delivery":
        raise RuntimeError("Local tools require APP_ENV=local and PROJECT_ID=demo-food-delivery.")
    if os.environ.get("FIRESTORE_EMULATOR_HOST") not in {
        "firestore:8080", "localhost:8080", "127.0.0.1:8080"
    }:
        raise RuntimeError("Local tools require the local Firestore emulator.")
    if os.environ.get("DB_PRIVATE_IP", "mysql") not in {"mysql", "localhost", "127.0.0.1"}:
        raise RuntimeError("Local tools require a local MySQL host.")
    return "demo-food-delivery"


def require_approved_recovery_root(root):
    """Require explicit approval of this checkout before any failure injection."""
    candidate = Path(root).resolve()
    checkout = Path(__file__).resolve().parents[1]
    approved = os.environ.get("FOOD_DELIVERY_RECOVERY_ROOT", "")
    if (
        not approved
        or not Path(approved).is_absolute()
        or Path(approved).resolve() != candidate
        or candidate != checkout
    ):
        raise RuntimeError(
            "Set FOOD_DELIVERY_RECOVERY_ROOT to this checkout's absolute path "
            "before running opt-in failure tests."
        )
    compose = candidate / "compose.yaml"
    if not compose.is_file() or compose.read_text(encoding="utf-8").splitlines()[:1] != [
        "name: cloudproject1-local"
    ]:
        raise RuntimeError("Recovery tests require the original local Compose project.")
    return candidate
