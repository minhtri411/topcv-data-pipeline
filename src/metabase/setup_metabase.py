import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

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


CARD_MANIFEST: List[Dict[str, Any]] = [
    # ===================== Tab 1: Tổng quan Thị trường =====================
    {
        "file": "00_kpi_total_jobs.sql",
        "name": "Tổng số Tin đã thu thập (toàn bộ lịch sử)",
        "display": "scalar",
        "tab": "Tổng quan Thị trường",
        "position": {"row": 0, "col": 0, "size_x": 6, "size_y": 2},
    },
    {
        "file": "00_kpi_jobs_latest_crawl.sql",
        "name": "Tin xuất hiện trong crawl gần nhất",
        "display": "scalar",
        "tab": "Tổng quan Thị trường",
        "position": {"row": 0, "col": 6, "size_x": 6, "size_y": 2},
    },
    {
        "file": "00_kpi_total_companies.sql",
        "name": "Tổng số Doanh nghiệp đang tuyển",
        "display": "scalar",
        "tab": "Tổng quan Thị trường",
        "position": {"row": 0, "col": 12, "size_x": 6, "size_y": 2},
    },
    {
        "file": "00_kpi_avg_salary.sql",
        "name": "Mức lương trung bình toàn thị trường",
        "display": "scalar",
        "tab": "Tổng quan Thị trường",
        "position": {"row": 0, "col": 18, "size_x": 6, "size_y": 2},
    },
    {
        "file": "06_hiring_trend_over_time.sql",
        "name": "Xu hướng tuyển dụng và biến động lương (30 ngày gần nhất)",
        "display": "combo",
        "tab": "Tổng quan Thị trường",
        "position": {"row": 2, "col": 0, "size_x": 24, "size_y": 6},
        "viz_settings": {
            "series_settings": {
                "Số tin đang tuyển": {
                    "display": "bar",
                    "axis": "left",
                    "color": "#A9D6A5",
                },
                "Lương trung bình (triệu VNĐ)": {
                    "display": "line",
                    "axis": "right",
                    "color": "#2CA8B0",
                },
            },
            "graph.show_values": True,
            "graph.y_axis.auto_range": False,
            "graph.y_axis.min": 0,
        },
    },
    {
        "file": "03_hotspots_by_location.sql",
        "name": "Top khu vực có nhu cầu tuyển dụng Data cao nhất",
        "display": "row",
        "tab": "Tổng quan Thị trường",
        "position": {"row": 8, "col": 0, "size_x": 12, "size_y": 6},
        "viz_settings": {
            "graph.show_values": True,
            "series_settings": {"Số lượng tin": {"color": "#4C9A6B"}},
        },
    },
    {
        "file": "08_job_appearance_status.sql",
        "name": "Tin còn xuất hiện ở lần crawl gần nhất (tín hiệu, không phải trạng thái đóng/mở)",
        "display": "pie",
        "tab": "Tổng quan Thị trường",
        "position": {"row": 8, "col": 12, "size_x": 12, "size_y": 6},
        "viz_settings": {
            "pie.colors": {
                "Còn xuất hiện": "#2ECC71",
                "Không còn xuất hiện": "#BDC3C7"
            }
        }
    },
    # ============ Tab 2: Phân tích Từ khóa & Mức lương  ============
    {
        "file": "02_salary_benchmark_by_experience.sql",
        "name": "Mặt bằng lương trung bình theo cấp bậc",
        "display": "combo",
        "tab": "Từ khóa & Mức lương",
        "position": {"row": 0, "col": 0, "size_x": 24, "size_y": 6},
        "viz_settings": {
            "series_settings": {
                "Số lượng tin": {
                    "display": "bar",
                    "axis": "right",
                    "color": "#A9D6A5",
                },
                "Mức lương trung bình (triệu VNĐ)": {
                    "display": "line",
                    "axis": "left",
                    "color": "#2CA8B0",
                },
                "Mức lương khởi điểm trung bình (triệu VNĐ)": {
                    "display": "line",
                    "axis": "left",
                    "color": "#7FCDD1",
                },
                "Mức lương trần trung bình (triệu VNĐ)": {
                    "display": "line",
                    "axis": "left",
                    "color": "#146A70",
                },
            },
            "graph.y_axis.auto_range": False,
            "graph.y_axis.min": 0,
        },
    },
    {
        "file": "01_top_recruitment_keywords.sql",
        "name": "Top 20 từ khóa tuyển dụng phổ biến nhất",
        "display": "row",
        "tab": "Từ khóa & Mức lương",
        "position": {"row": 6, "col": 0, "size_x": 24, "size_y": 10},
        "viz_settings": {
            "graph.show_values": True,
            "series_settings": {"Số lượng tin": {"color": "#4C9A6B"}},
        },
    },
    # ================ Tab 3: Phân tích Doanh nghiệp (Company Insights) ================
    {
        "file": "07_company_industry_distribution.sql",
        "name": "Phân bố tin tuyển dụng theo lĩnh vực hoạt động",
        "display": "row",
        "tab": "Doanh nghiệp",
        "position": {"row": 0, "col": 0, "size_x": 24, "size_y": 6},
        "viz_settings": {
            "graph.show_values": True,
            "series_settings": {
                "Số lượng tin": {"color": "#4C9A6B"},
                "Không rõ": {"color": "#B0B0B0"},
            },
        },
    },
    {
        "file": "04_company_size_distribution.sql",
        "name": "Phân bố tin tuyển dụng theo quy mô doanh nghiệp",
        "display": "bar",
        "tab": "Doanh nghiệp",
        "position": {"row": 6, "col": 0, "size_x": 24, "size_y": 6},
        "viz_settings": {
            "graph.show_values": True,
            "series_settings": {
                "Số lượng tin": {"color": "#4C9A6B"},
                "Không rõ": {"color": "#B0B0B0"},
            }
        },
    },
    {
        "file": "05_top_hiring_companies_leaderboard.sql",
        "name": "Top 10 Doanh nghiệp tuyển dụng nhiều nhất",
        "display": "table",
        "tab": "Doanh nghiệp",
        "position": {"row": 12, "col": 0, "size_x": 24, "size_y": 7},
        "viz_settings": {
            "table.column_widths": [40, None, None, None],
            "column_settings": {
                '["name","Số tin đang tuyển"]': {"show_mini_bar": True}
            },
        },
    },
]

DASHBOARD_NAME = "TopCV Analytics Overview"


def load_sql_manifest(sql_dir: str) -> None:
    for entry in CARD_MANIFEST:
        path = os.path.join(sql_dir, entry["file"])
        if not os.path.isfile(path):
            raise FileNotFoundError(f"SQL file listed in CARD_MANIFEST not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            entry["sql"] = f.read()


def find_card_by_name(base_url: str, session_token: str, name: str) -> Optional[int]:
    url = f"{base_url}/api/card"
    headers = {"X-Metabase-Session": session_token}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    for item in r.json():
        if item.get("name") == name:
            return int(item["id"])
    return None


def create_or_update_card(
    base_url: str,
    session_token: str,
    db_id: int,
    name: str,
    sql: str,
    display: str,
    viz_settings: Optional[Dict[str, Any]] = None,
) -> int:
    headers = {"X-Metabase-Session": session_token}
    payload = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": db_id,
        },
        "display": display,
        "visualization_settings": viz_settings or {},
    }

    existing_id = find_card_by_name(base_url, session_token, name)
    if existing_id is not None:
        r = requests.put(f"{base_url}/api/card/{existing_id}", headers=headers, json=payload, timeout=30)
        if not r.ok:
            raise RuntimeError(f"Failed to update card '{name}': {r.status_code} {r.text}")
        print(f"[OK] Updated card '{name}' (id={existing_id})")
        return existing_id

    r = requests.post(f"{base_url}/api/card", headers=headers, json=payload, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Failed to create card '{name}': {r.status_code} {r.text}")
    card_id = int(r.json()["id"])
    print(f"[OK] Created card '{name}' (id={card_id})")
    return card_id


def get_dashboard_tabs(base_url: str, session_token: str, dashboard_id: int) -> Dict[str, int]:
    headers = {"X-Metabase-Session": session_token}
    r = requests.get(f"{base_url}/api/dashboard/{dashboard_id}", headers=headers, timeout=20)
    r.raise_for_status()
    return {t["name"]: int(t["id"]) for t in (r.json().get("tabs") or [])}


def find_dashboard_by_name(base_url: str, session_token: str, name: str) -> Optional[int]:
    url = f"{base_url}/api/dashboard"
    headers = {"X-Metabase-Session": session_token}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    for item in r.json():
        if item.get("name") == name:
            return int(item["id"])
    return None


def create_or_get_dashboard(base_url: str, session_token: str, name: str) -> int:
    headers = {"X-Metabase-Session": session_token}
    existing_id = find_dashboard_by_name(base_url, session_token, name)
    if existing_id is not None:
        print(f"[INFO] Dashboard '{name}' already exists (id={existing_id})")
        r = requests.put(
            f"{base_url}/api/dashboard/{existing_id}",
            headers=headers,
            json={"width": "full"},
            timeout=30,
        )
        if not r.ok:
            raise RuntimeError(f"Failed to set width on dashboard '{name}': {r.status_code} {r.text}")
        return existing_id

    r = requests.post(
        f"{base_url}/api/dashboard",
        headers=headers,
        json={"name": name, "width": "full"},
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Failed to create dashboard '{name}': {r.status_code} {r.text}")
    dashboard_id = int(r.json()["id"])
    print(f"[OK] Created dashboard '{name}' (id={dashboard_id}, width=full)")
    return dashboard_id


def set_dashboard_cards(
    base_url: str,
    session_token: str,
    dashboard_id: int,
    card_entries: List[Tuple[int, Dict[str, int], str]],
) -> None:
    """card_entries: list of (card_id, position, tab_name)."""
    headers = {"X-Metabase-Session": session_token}

    existing_tabs = get_dashboard_tabs(base_url, session_token, dashboard_id)
    tab_names_in_order: List[str] = []
    for _, _, tab_name in card_entries:
        if tab_name not in tab_names_in_order:
            tab_names_in_order.append(tab_name)

    tab_id_by_name: Dict[str, int] = {}
    tabs_payload = []
    next_new_tab_id = -1
    for name in tab_names_in_order:
        tid = existing_tabs.get(name, next_new_tab_id)
        if name not in existing_tabs:
            next_new_tab_id -= 1
        tab_id_by_name[name] = tid
        tabs_payload.append({"id": tid, "name": name})

    dashcards = [
        {
            "id": -(i + 1),  # negative placeholder id signals "new" to Metabase
            "card_id": card_id,
            "row": pos["row"],
            "col": pos["col"],
            "size_x": pos["size_x"],
            "size_y": pos["size_y"],
            "dashboard_tab_id": tab_id_by_name[tab_name],
        }
        for i, (card_id, pos, tab_name) in enumerate(card_entries)
    ]
    r = requests.put(
        f"{base_url}/api/dashboard/{dashboard_id}/cards",
        headers=headers,
        json={"cards": dashcards, "tabs": tabs_payload},
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"Failed to set dashboard layout: {r.status_code} {r.text}")
    print(f"[OK] Dashboard layout set ({len(dashcards)} cards across {len(tabs_payload)} tabs)")


def provision_dashboard(base_url: str, session_token: str, db_id: int, sql_dir: str) -> None:
    load_sql_manifest(sql_dir)

    card_entries: List[Tuple[int, Dict[str, int], str]] = []
    for entry in CARD_MANIFEST:
        card_id = create_or_update_card(
            base_url,
            session_token,
            db_id,
            entry["name"],
            entry["sql"],
            entry["display"],
            entry.get("viz_settings"),
        )
        card_entries.append((card_id, entry["position"], entry["tab"]))

    dashboard_id = create_or_get_dashboard(base_url, session_token, DASHBOARD_NAME)
    set_dashboard_cards(base_url, session_token, dashboard_id, card_entries)
    print(f"[DONE] Dashboard '{DASHBOARD_NAME}' provisioned (id={dashboard_id})")


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

    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "metabase", "sql")
    sql_dir = os.getenv("METABASE_SQL_DIR", sql_dir)
    provision_dashboard(base_url, token, db_id, sql_dir)

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