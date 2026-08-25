import os
import re
from typing import Dict, List

BASE = "https://www.topcv.vn"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
}
REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 2

JOB_DETAIL_PATH_PATTERNS = [
    re.compile(r"^/viec-lam/.+?/\d+\.html$"),
    re.compile(r"^/brand/.+?/tuyen-dung/.+-j\d+\.html$"),
]

SEARCH_FIELD_MAP: Dict[str, List[str]] = {
    "title": [
        "h3.title a[href]",
        "a.job-item__title[href]",
        "a[href*='/viec-lam/']",
        "a[href*='/brand/'][href*='-j']",
    ],
    "salary": [
        "label.title-salary",
        ".title-salary",
        "[class*='salary']",
    ],
    "address_list": [
        "label.address .city-text",
        ".address .city-text",
        "[class*='location']",
        "[class*='address']",
    ],
    "exp_list": [
        "label.exp span",
        ".exp span",
        "[class*='experience']",
        "[class*='exp']",
    ],
    "company": [
        "a.company .company-name",
        "a.company",
        "[class*='company-name']",
    ],
    "company_url": [
        "a.company[href]::attr(href)",
        "a[href*='/cong-ty/']::attr(href)",
    ],
    "job_url": [
        "h3.title a[href]::attr(href)",
        "a[href*='/viec-lam/']::attr(href)",
        "a[href*='/brand/'][href*='-j']::attr(href)",
    ],
}

FIELD_MAP: Dict[str, List[str]] = {
    "title": [
        ".job-detail__info--title",
        "h1.job-title",
        "h1[class*='title']",
        "h1",
    ],
    "salary": [
        ".job-detail__info--section.section-salary .job-detail__info--section-content-value",
        ".job-detail__information-detail--actions .salary",
        ".job-overview [class*='salary']",
    ],
    "location": [
        ".job-detail__info--section.section-location .job-detail__info--section-content-value",
        ".job-detail__information-detail--actions .location",
        "[class*='job-location']",
    ],
    "experience": [
        ".job-detail__info--section.section-experience .job-detail__info--section-content-value",
        "[class*='experience']",
    ],
    "deadline": [
        ".job-detail__info--deadline-date",
        ".job-detail__info--deadline",
        ".job-detail__information-detail--actions-label",
        "[class*='deadline']",
    ],
    "tags": [
        ".job-tags a.item",
        ".job-tags .item",
        ".job-detail__info--tags a",
        "[class*='tag'] a",
    ],
    "company_url_from_job": [
        "a.company[href]::attr(href)",
        "a[href*='/cong-ty/']::attr(href)",
    ],
    "company_name_full": [
        ".company-name-label .name",
    ],
    "company_website": [
        "a.company-subdetail-info-text[href^='http']::attr(href)",
        ".company-subdetail-info a[href^='http']::attr(href)",
    ],
    "company_size": [
        ".company-scale .company-value",
    ],
    "company_followers": [
        ".company-subdetail-info:-soup-contains('Người theo dõi') .company-subdetail-info-text",
        ".info-item:-soup-contains('Người theo dõi') .value",
    ],
    "company_industry": [
        ".company-field .company-value",
    ],
    "company_address": [
        ".company-address .company-value",
    ],
    "company_description": [
        ".content",
        ".intro-content",
        ".intro-section .section-body",
        "div.company-description",
        "div#company-description",
        "div.box-intro-company",
        "div#readmore-company",
    ],
}

BASE_COLUMNS = [
    "title",
    "job_url",
    "company_url",
    "salary",
    "location",
    "experience",
    "deadline",
    "tags",
    "desc_mota",
    "desc_yeucau",
    "desc_quyenloi",
    "working_addresses",
    "working_times",
    "company_name_full",
    "company_website",
    "company_size",
    "company_followers",
    "company_industry",
    "company_address",
    "crawled_at",
    "company_description",
]


def _get_required_env(name: str) -> str:
    """Fetch a required env var; raise ValueError if missing or empty."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _resolve_project_dir() -> str:
    """Resolve the project root directory across local/Airflow/CLI runs."""
    configured = os.getenv("PROJECT_DIR", "").strip()

    if os.getenv("AIRFLOW_CTX_DAG_ID"):
        project_dir = _get_required_env("PROJECT_DIR")
        if not os.path.isdir(project_dir):
            raise ValueError(f"PROJECT_DIR does not exist in Airflow runtime: {project_dir}")
        return project_dir

    if os.getenv("FORCE_PROJECT_DIR", "").strip().lower() in {"1", "true", "yes"} and configured:
        return configured

    return os.getcwd()