import json
import os
import time
from typing import Any, Dict, Optional

import requests


def load_env_file(path: str = ".env") -> None:
    """Load key=value pairs from .env into os.environ if not already set."""
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def get_env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise ValueError(f"Missing required env var: {name}")
    return val


def wait_for_metabase(base_url: str, timeout_sec: int = 180) -> None:
    start = time.time()
    health_url = f"{base_url}/api/health"

    while time.time() - start < timeout_sec:
        try:
            r = requests.get(health_url, timeout=5)
            if r.ok:
                print("[OK] Metabase is ready")
                return
        except requests.RequestException:
            pass
        time.sleep(2)

    raise TimeoutError(f"Metabase is not ready after {timeout_sec}s: {health_url}")


def get_setup_token(base_url: str) -> Optional[str]:
    url = f"{base_url}/api/session/properties"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("setup-token")


def setup_first_time(base_url: str, payload: Dict[str, Any]) -> None:
    url = f"{base_url}/api/setup"
    r = requests.post(url, json=payload, timeout=30)
    if r.status_code == 403 and "first user" in r.text.lower():
        print("[INFO] Metabase already has a user, skip first-time setup")
        return
    if not r.ok:
        raise RuntimeError(f"Failed to run /api/setup: {r.status_code} {r.text}")
    print("[OK] First-time setup completed")


def login(base_url: str, username: str, password: str) -> str:
    url = f"{base_url}/api/session"
    payload = {"username": username, "password": password}
    r = requests.post(url, json=payload, timeout=15)
    if not r.ok:
        raise RuntimeError(
            "Failed to login Metabase. Check METABASE_ADMIN_EMAIL/METABASE_ADMIN_PASSWORD in .env "
            f"(status={r.status_code}, body={r.text})"
        )
    data = r.json()
    token = data.get("id")
    if not token:
        raise RuntimeError("Metabase login response missing session id")
    return token


def find_database_by_name(base_url: str, session_token: str, db_name: str) -> Optional[int]:
    url = f"{base_url}/api/database"
    headers = {"X-Metabase-Session": session_token}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()

    for item in r.json().get("data", []):
        if item.get("name") == db_name:
            return int(item["id"])
    return None


def create_database(base_url: str, session_token: str, payload: Dict[str, Any]) -> int:
    url = f"{base_url}/api/database"
    headers = {"X-Metabase-Session": session_token}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if not r.ok:
        if r.status_code == 400 and "password" in r.text.lower():
            raise RuntimeError(
                "Failed to create Metabase database due to credential mismatch. "
                "Check METABASE_TARGET_DB_USER/METABASE_TARGET_DB_PASSWORD in .env. "
                "If PostgreSQL data volume was created earlier with different credentials, "
                "the current .env DB_* values may not match the running database. "
                f"Details: {r.status_code} {r.text}"
            )
        raise RuntimeError(f"Failed to create Metabase database: {r.status_code} {r.text}")
    db_id = int(r.json()["id"])
    print(f"[OK] Created Metabase database id={db_id}")
    return db_id


def sync_database_schema(base_url: str, session_token: str, db_id: int) -> None:
    url = f"{base_url}/api/database/{db_id}/sync_schema"
    headers = {"X-Metabase-Session": session_token}
    r = requests.post(url, headers=headers, timeout=20)
    if not r.ok:
        raise RuntimeError(f"Failed to sync schema for database {db_id}: {r.status_code} {r.text}")
    print(f"[OK] Triggered schema sync for database id={db_id}")


def build_connection_details() -> Dict[str, Any]:
    return {
        "host": get_env("METABASE_TARGET_DB_HOST", get_env("DB_HOST", "postgres")),
        "port": int(get_env("METABASE_TARGET_DB_PORT", get_env("DB_PORT", "5432"))),
        "dbname": get_env("METABASE_TARGET_DB_NAME", get_env("DB_NAME", "topcv_dw")),
        "user": get_env("METABASE_TARGET_DB_USER", get_env("DB_USER", "topcv_user")),
        "password": get_env("METABASE_TARGET_DB_PASSWORD", get_env("DB_PASSWORD", "replace_with_strong_password")),
        "ssl": False,
        "tunnel-enabled": False,
    }


def validate_admin_password(password: str) -> None:
    # Metabase rejects very common/default passwords during first-time setup.
    blocked_values = {
        "replace_with_metabase_admin_password",
        "replace_with_admin_password",
        "admin",
        "password",
        "123456",
    }
    if password in blocked_values:
        raise ValueError(
            "METABASE_ADMIN_PASSWORD is a placeholder/weak value. "
            "Set a strong password in .env, then run setup again."
        )


def main() -> None:
    load_env_file()

    base_url = get_env("METABASE_URL", "http://localhost:3000").rstrip("/")
    admin_email = get_env("METABASE_ADMIN_EMAIL", "admin@topcv.local")
    admin_password = get_env("METABASE_ADMIN_PASSWORD", "replace_with_admin_password")
    admin_first_name = get_env("METABASE_ADMIN_FIRST_NAME", "TopCV")
    admin_last_name = get_env("METABASE_ADMIN_LAST_NAME", "Admin")
    site_name = get_env("METABASE_SITE_NAME", "TopCV Analytics")
    metabase_db_name = get_env("METABASE_DATABASE_DISPLAY_NAME", "TopCV Data Warehouse")

    validate_admin_password(admin_password)

    wait_for_metabase(base_url)
    setup_token = get_setup_token(base_url)
    conn_details = build_connection_details()

    if setup_token:
        setup_payload = {
            "token": setup_token,
            "prefs": {
                "site_name": site_name,
                "site_locale": "en",
                "allow_tracking": False,
            },
            "user": {
                "first_name": admin_first_name,
                "last_name": admin_last_name,
                "email": admin_email,
                "password": admin_password,
            },
            "database": {
                "name": metabase_db_name,
                "engine": "postgres",
                "details": conn_details,
                "is_full_sync": True,
                "is_on_demand": False,
                "auto_run_queries": True,
            },
        }
        setup_first_time(base_url, setup_payload)
    else:
        print("[INFO] Metabase already initialized, skipping /api/setup")

    token = login(base_url, admin_email, admin_password)
    db_id = find_database_by_name(base_url, token, metabase_db_name)

    if db_id is None:
        create_payload = {
            "name": metabase_db_name,
            "engine": "postgres",
            "details": conn_details,
            "is_full_sync": True,
            "is_on_demand": False,
            "auto_run_queries": True,
        }
        db_id = create_database(base_url, token, create_payload)
    else:
        print(f"[INFO] Database already exists in Metabase id={db_id}")

    sync_database_schema(base_url, token, db_id)
    print("[DONE] Metabase bootstrap finished")
    print(json.dumps({
        "metabase_url": base_url,
        "database_id": db_id,
        "database_name": metabase_db_name,
        "target_db": conn_details["dbname"],
        "target_host": conn_details["host"],
    }, indent=2))


if __name__ == "__main__":
    main()
