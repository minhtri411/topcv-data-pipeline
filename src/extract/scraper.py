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
from curl_cffi import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE = "https://www.topcv.vn"
DEFAULT_IMPERSONATE = "chrome120"
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 5

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

LIST_FIELDS = {"tags", "working_addresses", "working_times"}


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _resolve_project_dir() -> str:
    configured = os.getenv("PROJECT_DIR", "").strip()

    if os.getenv("AIRFLOW_CTX_DAG_ID"):
        project_dir = _get_required_env("PROJECT_DIR")
        if not os.path.isdir(project_dir):
            raise ValueError(f"PROJECT_DIR does not exist in Airflow runtime: {project_dir}")
        return project_dir

    if os.getenv("FORCE_PROJECT_DIR", "").strip().lower() in {"1", "true", "yes"} and configured:
        return configured

    return os.getcwd()


def text(el) -> Optional[str]:
    if not el:
        return None
    value = el.get_text(" ", strip=True)
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip()


def smart_sleep(min_s: float = 1.5, max_s: float = 3.0) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _normalize_job_title(raw_title: Optional[str]) -> Optional[str]:
    if not raw_title:
        return None
    cleaned = re.sub(r"\s+", " ", raw_title).strip()
    cleaned = cleaned.replace("HOT", "").replace("✨", "").strip()
    return cleaned or None


def _parse_selector(selector: str) -> Tuple[str, Optional[str]]:
    attr_match = re.search(r"::attr\(([^)]+)\)$", selector)
    if not attr_match:
        return selector, None
    return selector[: selector.rfind("::attr(")], attr_match.group(1).strip()


def apply_map(soup: BeautifulSoup, selectors: List[str], multi: bool = False) -> Optional[str]:
    for selector in selectors:
        css_selector, attr_name = _parse_selector(selector)
        try:
            nodes = soup.select(css_selector)
        except Exception:
            continue

        if not nodes:
            continue

        if multi:
            values: List[str] = []
            seen = set()
            for node in nodes:
                raw = node.get(attr_name) if attr_name else text(node)
                if not raw:
                    continue
                value = re.sub(r"\s+", " ", str(raw)).strip()
                if value and value not in seen:
                    seen.add(value)
                    values.append(value)
            if values:
                return "; ".join(values)
            continue

        for node in nodes:
            raw = node.get(attr_name) if attr_name else text(node)
            if raw:
                value = re.sub(r"\s+", " ", str(raw)).strip()
                if value:
                    return value
    return None


def get_value_by_label(soup: BeautifulSoup, label_texts: List[str]) -> Optional[str]:
    label_norms = [re.sub(r"\s+", " ", label).strip().lower() for label in label_texts]
    for section in soup.select(".job-detail__info--section, .job-overview__item, .job-info-item"):
        title_node = section.select_one(".job-detail__info--section-content-title, .title, .label, h3, h4, strong")
        value_node = section.select_one(".job-detail__info--section-content-value, .value, .content")
        title_text = text(title_node)
        if not title_text:
            continue
        normalized_title = re.sub(r"\s+", " ", title_text).strip().lower()
        if any(label_norm in normalized_title for label_norm in label_norms):
            value_text = text(value_node)
            if value_text:
                return value_text
            section_text = text(section)
            if section_text:
                cleaned = re.sub(rf"^{re.escape(title_text)}\s*[:：-]?\s*", "", section_text, flags=re.I).strip()
                return cleaned or section_text
    return apply_map(
        soup,
        [
            *[f".job-detail__info--section:-soup-contains('{label}') .job-detail__info--section-content-value" for label in label_texts],
            *[f".job-overview__item:-soup-contains('{label}') .value" for label in label_texts],
            *[f".job-info-item:-soup-contains('{label}') .value" for label in label_texts],
        ],
    )


def extract_description_sections(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    buckets = {
        "desc_mota": [],
        "desc_yeucau": [],
        "desc_quyenloi": [],
        "working_addresses": [],
        "working_times": [],
    }

    for item in soup.select(".job-description__item"):
        heading = text(item.select_one("h3, h2")) or ""
        heading_norm = re.sub(r"\s+", " ", heading).strip().lower()
        content = text(item.select_one(".job-description__item--content")) or text(item)
        if not content:
            continue
        if heading:
            content = re.sub(rf"^{re.escape(heading)}\s*", "", content, flags=re.I).strip() or content

        if "mô tả" in heading_norm or "mo ta" in heading_norm:
            buckets["desc_mota"].append(content)
        elif "yêu cầu" in heading_norm or "yeu cau" in heading_norm:
            buckets["desc_yeucau"].append(content)
        elif "quyền lợi" in heading_norm or "quyen loi" in heading_norm or "phúc lợi" in heading_norm or "phuc loi" in heading_norm:
            buckets["desc_quyenloi"].append(content)
        elif "địa điểm làm việc" in heading_norm or "dia diem lam viec" in heading_norm:
            buckets["working_addresses"].append(content)
        elif "thời gian làm việc" in heading_norm or "thoi gian lam viec" in heading_norm:
            buckets["working_times"].append(content)

    return {key: "; ".join(values) if values else None for key, values in buckets.items()}


def extract_deadline(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", value)
    return match.group(1) if match else value


def build_session() -> requests.Session:
    return requests.Session(impersonate=DEFAULT_IMPERSONATE)


def warmup_session(session: requests.Session) -> None:
    try:
        session.get(BASE, timeout=REQUEST_TIMEOUT_SECONDS)
        smart_sleep(0.8, 1.5)
    except Exception as exc:
        logger.warning("Warmup failed: %s", str(exc)[:200])


def get_html(session: requests.Session, url: str) -> Optional[str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code in (403, 429) or "<title>Just a moment...</title>" in response.text:
                logger.warning("Cloudflare/blocked page at URL: %s (attempt %s)", url, attempt)
                if attempt == MAX_RETRIES:
                    return None
                time.sleep(random.uniform(5, 10))
                logger.info("Renewing session cookie to bypass Cloudflare...")
                warmup_session(session)
                continue
            response.raise_for_status()
            return response.text
        except Exception as exc:
            logger.warning("Fetch failed at URL: %s (attempt %s): %s", url, attempt, str(exc)[:200])
            if attempt == MAX_RETRIES:
                return None
            time.sleep(random.uniform(5, 10))
    return None


def _is_job_detail_href(href: Optional[str]) -> bool:
    if not href:
        return False
    parsed = urlparse(href)
    path = parsed.path if parsed.path else href
    return any(pattern.match(path) for pattern in JOB_DETAIL_PATH_PATTERNS)


def parse_search_page(html: str) -> Tuple[List[Dict], bool]:
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

        job_url = urljoin(BASE, job_url_raw)
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
                "company_url": urljoin(BASE, company_url_raw) if company_url_raw else None,
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
            job_url = urljoin(BASE, href)
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

    has_next = any(
        soup.select(selector)
        for selector in [
            "a[rel='next'][href]",
            "li.page-item.next a[href]",
            "a.next-page[href]",
            "a.pagination-next[href]",
            "a[aria-label='Next'][href]",
        ]
    )

    return jobs, has_next


def scrape_job_detail(session: requests.Session, job_url: str) -> Dict:
    html = get_html(session, job_url)
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    detail: Dict[str, Optional[str]] = {
        "title": apply_map(soup, FIELD_MAP["title"]),
        "salary": get_value_by_label(soup, ["mức lương", "thu nhập"]),
        "location": get_value_by_label(soup, ["địa điểm"]),
        "experience": get_value_by_label(soup, ["kinh nghiệm"]),
        "deadline": extract_deadline(apply_map(soup, FIELD_MAP["deadline"])),
        "tags": apply_map(soup, FIELD_MAP["tags"], multi=True),
        "company_url_from_job": apply_map(soup, FIELD_MAP["company_url_from_job"]),
        "company_name_full": apply_map(soup, FIELD_MAP["company_name_full"]),
        "company_website": apply_map(soup, FIELD_MAP["company_website"]),
        "company_size": apply_map(soup, FIELD_MAP["company_size"]),
        "company_followers": apply_map(soup, FIELD_MAP["company_followers"]),
        "company_industry": apply_map(soup, FIELD_MAP["company_industry"]),
        "company_address": apply_map(soup, FIELD_MAP["company_address"]),
    }

    sections = extract_description_sections(soup)
    detail.update(sections)

    detail["title"] = _normalize_job_title(detail.get("title"))
    return detail


def _extract_company_value_from_label_rows(soup: BeautifulSoup, labels: List[str]) -> Optional[str]:
    label_norms = [re.sub(r"\s+", " ", label).strip().lower() for label in labels]
    for row in soup.select("li, .row, .item, .info-item, .company-info-item, .dl, .d-flex"):
        row_text = text(row) or ""
        strong = row.find(["strong", "b"])
        if strong:
            label = text(strong) or ""
            value = re.sub(re.escape(label), "", row_text, flags=re.I).strip(" :-–—")
            if any(label_norm in label.lower() for label_norm in label_norms) and value:
                return value
            continue
        m = re.match(r"^([^:：]+)[:：]\s*(.+)$", row_text)
        if not m:
            continue
        label = re.sub(r"\s+", " ", m.group(1)).strip().lower()
        value = m.group(2).strip()
        if any(label_norm in label for label_norm in label_norms):
            return value
    return None


def _is_noise_company_description(value: Optional[str]) -> bool:
    if not value:
        return True
    normalized = re.sub(r"\s+", " ", value).strip().lower()
    if not normalized:
        return True
    noise_markers = [
        "chia sẻ vị trí",
        "rất tiếc vì trải nghiệm",
        "gửi phản hồi",
        "đâu là yếu tố khiến bạn cảm thấy chưa hài lòng",
    ]
    return any(marker in normalized for marker in noise_markers)


def scrape_company(session: requests.Session, company_url: Optional[str]) -> Dict:
    if not company_url:
        return {
            "company_name_full": None,
            "company_website": None,
            "company_size": None,
            "company_followers": None,
            "company_industry": None,
            "company_address": None,
            "company_description": None,
        }

    html = get_html(session, company_url)
    if not html:
        return {
            "company_name_full": None,
            "company_website": None,
            "company_size": None,
            "company_followers": None,
            "company_industry": None,
            "company_address": None,
            "company_description": None,
        }

    soup = BeautifulSoup(html, "lxml")

    company_name = apply_map(soup, FIELD_MAP["company_name_full"])
    if not company_name:
        h1_nodes = soup.select("h1")
        if len(h1_nodes) == 1:
            company_name = text(h1_nodes[0])
        else:
            company_name = text(soup.select_one("h1.company-name, h1.title, div.company-header h1, div.company-info h1"))

    website = apply_map(soup, FIELD_MAP["company_website"])
    if website and not str(website).startswith("http"):
        website = None

    size = apply_map(soup, FIELD_MAP["company_size"])
    if not size:
        size = _extract_company_value_from_label_rows(soup, ["quy mô", "nhân viên", "size"])

    followers = apply_map(soup, FIELD_MAP["company_followers"])
    industry = apply_map(soup, FIELD_MAP["company_industry"])
    if not industry:
        industry = _extract_company_value_from_label_rows(soup, ["lĩnh vực", "ngành nghề", "industry"])

    address = apply_map(soup, FIELD_MAP["company_address"])
    if not address:
        address = _extract_company_value_from_label_rows(soup, ["địa chỉ", "address"])

    description = apply_map(
        soup,
        [
            "#section-introduce .box-body .content",
            "#section-introduce .content",
            "#section-introduce .box-body",
            "div.company-info #section-introduce .content",
            *FIELD_MAP["company_description"],
        ],
    )

    if not description:
        for heading in soup.select("#section-introduce h1, #section-introduce h2, #section-introduce h3, .company-info h1, .company-info h2, .company-info h3"):
            heading_text = (text(heading) or "").lower()
            if "giới thiệu" not in heading_text and "gioi thieu" not in heading_text:
                continue
            section = heading.find_parent("div")
            while section is not None:
                candidate = text(section.select_one(".content, .box-body")) or text(section)
                if candidate:
                    description = candidate
                    break
                section = section.find_parent("div")
            if description:
                break

    if _is_noise_company_description(description):
        description = None

    smart_sleep(1.5, 3.0)

    return {
        "company_name_full": company_name,
        "company_website": website,
        "company_size": size,
        "company_followers": followers,
        "company_industry": industry,
        "company_address": address,
        "company_description": description,
    }


def _coalesce(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def _merge_row(base_job: Dict, detail: Dict, company: Dict) -> Dict:
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
    session = build_session()
    warmup_session(session)

    rows: List[Dict] = []
    seen_jobs = set()

    for page in range(start_page, end_page + 1):
        url = query_url_template.format(page=page)
        logger.info("Page %s started: %s", page, url)

        html = get_html(session, url)
        if not html:
            logger.warning("Page %s has no HTML, stopping", page)
            break

        jobs, has_next = parse_search_page(html)
        if not jobs:
            logger.warning("Page %s returned no jobs, stopping", page)
            break

        page_rows = []
        for job in jobs:
            job_id = urlparse(job["job_url"]).path
            if job_id in seen_jobs:
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
            if company_url:
                smart_sleep(*delay_between_pages)
            row = _merge_row(job, detail, company)
            page_rows.append(row)
            rows.append(row)

        logger.info("Page %s success %s/%s", page, len(page_rows), len(jobs))
        smart_sleep(*delay_between_pages)

        if not has_next and page < end_page:
            logger.info(
                "Page %s has no next-link marker, but continuing until configured end_page=%s",
                page,
                end_page,
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    cols = [col for col in BASE_COLUMNS if col in df.columns]
    return df.loc[:, cols] if cols else df


def main() -> None:
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
