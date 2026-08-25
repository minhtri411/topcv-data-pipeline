import logging
import os
import random
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup

from config import BASE, BASE_COLUMNS, JOB_DETAIL_PATH_PATTERNS, SEARCH_FIELD_MAP, _get_required_env, _resolve_project_dir
from extractors import _normalize_job_title, _normalize_topcv_url, apply_map, scrape_company, scrape_job_detail, text
from http_client import build_session, get_html, smart_sleep, warmup_session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _is_job_detail_href(href: Optional[str]) -> bool:
    """Check whether a href matches a known job-detail URL pattern."""
    if not href:
        return False
    parsed = urlparse(href)
    path = parsed.path if parsed.path else href
    return any(pattern.match(path) for pattern in JOB_DETAIL_PATH_PATTERNS)


def parse_search_page(html: str) -> List[Dict]:
    """Parse a search-results page HTML into a list of job listing dicts."""
    soup = BeautifulSoup(html, "lxml")
    jobs: List[Dict] = []

    card_selectors = [
        "div.job-item-search-result",
        "div.job-card",
        "div[class*='job-item']",
        "div[class*='job-card']",
        "section[class*='job']",
    ]

    cards = []
    for selector in card_selectors:
        matched = soup.select(selector)
        if matched:
            cards = matched
            break

    seen_urls = set()
    for card in cards:
        job_url_raw = apply_map(card, SEARCH_FIELD_MAP["job_url"])
        if not _is_job_detail_href(job_url_raw):
            continue

        job_url = _normalize_topcv_url(urljoin(BASE, job_url_raw))
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        title = _normalize_job_title(apply_map(card, SEARCH_FIELD_MAP["title"]))
        if not title:
            continue

        company_url_raw = apply_map(card, SEARCH_FIELD_MAP["company_url"])
        jobs.append(
            {
                "title": title,
                "job_url": job_url,
                "company": apply_map(card, SEARCH_FIELD_MAP["company"]),
                "company_url": _normalize_topcv_url(urljoin(BASE, company_url_raw)) if company_url_raw else None,
                "salary": apply_map(card, SEARCH_FIELD_MAP["salary"]),
                "address_list": apply_map(card, SEARCH_FIELD_MAP["address_list"]),
                "exp_list": apply_map(card, SEARCH_FIELD_MAP["exp_list"]),
            }
        )

    if not jobs:
        for anchor in soup.select("a[href]"):
            href = anchor.get("href")
            if not _is_job_detail_href(href):
                continue
            job_url = _normalize_topcv_url(urljoin(BASE, href))
            if job_url in seen_urls:
                continue
            title = _normalize_job_title(text(anchor))
            if not title:
                continue
            seen_urls.add(job_url)
            jobs.append(
                {
                    "title": title,
                    "job_url": job_url,
                    "company": None,
                    "company_url": None,
                    "salary": None,
                    "address_list": None,
                    "exp_list": None,
                }
            )

    return jobs




def _coalesce(*values: Optional[str]) -> Optional[str]:
    """Return the first non-null, non-empty value from the arguments."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def _merge_row(base_job: Dict, detail: Dict, company: Dict) -> Dict:
    """Merge listing + job-detail + company data into one output row."""
    company_url = _coalesce(base_job.get("company_url"), detail.get("company_url_from_job"))

    row = {
        "title": _coalesce(detail.get("title"), base_job.get("title")),
        "job_url": base_job.get("job_url"),
        "company_url": company_url,
        "salary": _coalesce(detail.get("salary"), base_job.get("salary")),
        "location": _coalesce(detail.get("location"), base_job.get("address_list")),
        "experience": _coalesce(detail.get("experience"), base_job.get("exp_list")),
        "deadline": detail.get("deadline"),
        "tags": detail.get("tags"),
        "desc_mota": detail.get("desc_mota"),
        "desc_yeucau": detail.get("desc_yeucau"),
        "desc_quyenloi": detail.get("desc_quyenloi"),
        "working_addresses": detail.get("working_addresses"),
        "working_times": detail.get("working_times"),
        "company_name_full": _coalesce(detail.get("company_name_full"), company.get("company_name_full"), base_job.get("company")),
        "company_website": _coalesce(detail.get("company_website"), company.get("company_website"), company_url),
        "company_size": _coalesce(company.get("company_size"), detail.get("company_size")),
        "company_followers": _coalesce(company.get("company_followers"), detail.get("company_followers")),
        "company_industry": _coalesce(company.get("company_industry"), detail.get("company_industry")),
        "company_address": _coalesce(company.get("company_address"), detail.get("company_address")),
        "company_description": _coalesce(company.get("company_description"), detail.get("company_description")),
        "crawled_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

    for key, value in row.items():
        if value is None:
            row[key] = ""

    return row


def crawl_to_dataframe(
    query_url_template: str,
    start_page: int = 1,
    end_page: int = 1,
    delay_between_pages: Tuple[float, float] = (1.5, 3.0),
) -> pd.DataFrame:
    """Crawl search result pages end-to-end and return the collected rows as a DataFrame."""
    session = build_session()
    warmup_session(session)

    rows: List[Dict] = []
    seen_jobs = set()

    for page in range(start_page, end_page + 1):
        url = query_url_template.format(page=page)
        logger.info("Page %s started: %s", page, url)

        html = get_html(session, url)
        if not html:
            logger.warning("Page %s failed, giving listing page one extra attempt after a longer cool-down", page)
            time.sleep(random.uniform(20, 30))
            html = get_html(session, url)
        if not html:
            logger.warning(
                "Page %s has no HTML, stopping EARLY (%s/%s configured pages completed, "
                "%s rows collected so far). Likely persistent Cloudflare block.",
                page, page - start_page, end_page - start_page + 1, len(rows),
            )
            break

        jobs = parse_search_page(html)
        if not jobs:
            logger.warning("Page %s returned no jobs, stopping", page)
            break

        page_rows = []
        duplicate_count = 0
        for job in jobs:
            job_id = urlparse(job["job_url"]).path
            if job_id in seen_jobs:
                duplicate_count += 1
                continue
            seen_jobs.add(job_id)

            detail = scrape_job_detail(session, job["job_url"])
            smart_sleep(*delay_between_pages)
            company_url = _coalesce(job.get("company_url"), detail.get("company_url_from_job"))
            company = scrape_company(session, company_url) if company_url else {
                "company_name_full": None,
                "company_website": None,
                "company_size": None,
                "company_followers": None,
                "company_industry": None,
                "company_address": None,
                "company_description": None,
            }
            row = _merge_row(job, detail, company)
            page_rows.append(row)
            rows.append(row)

        if duplicate_count:
            logger.info(
                "Page %s: %s new, %s already seen on an earlier page (listing overlap, not a failure)",
                page, len(page_rows), duplicate_count,
            )
        logger.info("Page %s success %s/%s", page, len(page_rows), len(jobs))
        smart_sleep(*delay_between_pages)

        if duplicate_count == len(jobs) and len(jobs) > 0:
            logger.info(
                "Page %s returned zero new jobs (all %s already seen) -- "
                "treating as the real end of the listing, stopping early instead of continuing to end_page=%s",
                page, duplicate_count, end_page,
            )
            break

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    cols = [col for col in BASE_COLUMNS if col in df.columns]
    return df.loc[:, cols] if cols else df


def main() -> None:
    """CLI/Airflow entrypoint: run the crawl and write the day's CSV to data/raw/."""
    query_template = "https://www.topcv.vn/tim-viec-lam-data?type_keyword=1&page={page}&sba=1"

    start_page = int(_get_required_env("SCRAPER_START_PAGE"))
    end_page = int(_get_required_env("SCRAPER_MAX_PAGES"))
    delay_min = float(_get_required_env("SCRAPER_PAGE_DELAY_MIN"))
    delay_max = float(_get_required_env("SCRAPER_PAGE_DELAY_MAX"))

    if delay_min <= 0 or delay_max <= 0 or delay_min > delay_max:
        raise ValueError("SCRAPER_PAGE_DELAY_MIN/MAX invalid")
    if end_page < start_page:
        raise ValueError("SCRAPER_MAX_PAGES must be >= SCRAPER_START_PAGE")

    logger.info(
        "Configuration: start_page=%s, max_pages=%s, delay_min=%s, delay_max=%s",
        start_page,
        end_page,
        delay_min,
        delay_max,
    )

    try:
        df = crawl_to_dataframe(
            query_template,
            start_page=start_page,
            end_page=end_page,
            delay_between_pages=(delay_min, delay_max),
        )
    except KeyboardInterrupt:
        logger.error("Fatal error: interrupted by user")
        sys.exit(1)
    except Exception as exc:
        logger.error("Fatal error: %s: %s", type(exc).__name__, str(exc))
        logger.error(traceback.format_exc())
        sys.exit(1)

    if df.empty:
        logger.error("Fatal error: no data collected")
        sys.exit(1)

    project_dir = _resolve_project_dir()
    output_dir = os.path.join(project_dir, "data", "raw")
    os.makedirs(output_dir, exist_ok=True)

    execution_date = os.getenv("EXECUTION_DATE")
    if not execution_date:
        if os.getenv("AIRFLOW_CTX_DAG_ID"):
            raise ValueError("Missing required environment variable: EXECUTION_DATE")
        if len(sys.argv) >= 2 and sys.argv[1].strip():
            execution_date = sys.argv[1].strip()
        else:
            execution_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    if not re.fullmatch(r"\d{8}", execution_date):
        raise ValueError("EXECUTION_DATE must be in YYYYMMDD format")

    csv_path = os.path.join(output_dir, f"topcv_jobs_{execution_date}.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info("Saved CSV: %s", csv_path)


if __name__ == "__main__":
    main()