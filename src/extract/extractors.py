import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config import BASE, FIELD_MAP, _resolve_project_dir
from http_client import get_html, smart_sleep

logger = logging.getLogger(__name__)


def text(el) -> Optional[str]:
    """Return normalized, whitespace-collapsed text from a BeautifulSoup element."""
    if not el:
        return None
    value = el.get_text(" ", strip=True)
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip()


def _normalize_job_title(raw_title: Optional[str]) -> Optional[str]:
    """Strip promotional markers (e.g. 'HOT', '✨') from a raw job title."""
    if not raw_title:
        return None
    cleaned = re.sub(r"\s+", " ", raw_title).strip()
    cleaned = cleaned.replace("HOT", "").replace("✨", "").strip()
    return cleaned or None


def _parse_selector(selector: str) -> Tuple[str, Optional[str]]:
    """Split a CSS selector into (selector, attribute) if it has an ::attr() suffix."""
    attr_match = re.search(r"::attr\(([^)]+)\)$", selector)
    if not attr_match:
        return selector, None
    return selector[: selector.rfind("::attr(")], attr_match.group(1).strip()


def apply_map(soup: BeautifulSoup, selectors: List[str], multi: bool = False) -> Optional[str]:
    """Try a list of CSS selectors in order, returning the first non-empty match."""
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
    """Find a field's value by matching its section/row label text (more resilient than a fixed CSS class)."""
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


def _match_description_label(heading_norm: str) -> Optional[str]:
    """Map a heading's normalized text to the description bucket it belongs to."""
    if "mô tả" in heading_norm or "mo ta" in heading_norm:
        return "desc_mota"
    if "yêu cầu" in heading_norm or "yeu cau" in heading_norm:
        return "desc_yeucau"
    if "quyền lợi" in heading_norm or "quyen loi" in heading_norm or "phúc lợi" in heading_norm or "phuc loi" in heading_norm:
        return "desc_quyenloi"
    if "địa điểm làm việc" in heading_norm or "dia diem lam viec" in heading_norm:
        return "working_addresses"
    if "thời gian làm việc" in heading_norm or "thoi gian lam viec" in heading_norm:
        return "working_times"
    return None


def extract_description_sections(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    """Extract mô tả/yêu cầu/quyền lợi/địa điểm/giờ làm việc from a job page, with a heading-text fallback for when the primary CSS selector misses a section."""
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
        bucket_key = _match_description_label(heading_norm)
        if bucket_key:
            buckets[bucket_key].append(content)

    for heading_tag in soup.select("h2, h3"):
        heading_norm = re.sub(r"\s+", " ", text(heading_tag) or "").strip().lower()
        bucket_key = _match_description_label(heading_norm)
        if not bucket_key:
            continue
        content_parts = []
        for sibling in heading_tag.find_next_siblings():
            stop_level = ("h1", "h2") if heading_tag.name == "h2" else ("h1", "h2", "h3")
            if sibling.name in stop_level:
                break
            
            nested_heading = sibling.find(["h1", "h2", "h3"])
            if nested_heading is not None:
                nested_norm = re.sub(r"\s+", " ", text(nested_heading) or "").strip().lower()
                if _match_description_label(nested_norm):
                    break
            sibling_text = text(sibling)
            if sibling_text:
                content_parts.append(sibling_text)
        content = "\n".join(content_parts).strip()
        if not content:
            continue
        current_len = sum(len(v) for v in buckets[bucket_key])
        if len(content) > current_len:
            buckets[bucket_key] = [content]

    return {key: "; ".join(values) if values else None for key, values in buckets.items()}


def _split_description_fragment(fragment_html: str) -> Dict[str, Optional[str]]:
    """Split a raw HTML fragment into desc_mota/desc_yeucau/desc_quyenloi buckets by heading text."""
    frag_soup = BeautifulSoup(fragment_html, "lxml")
    buckets: Dict[str, List[str]] = {"desc_mota": [], "desc_yeucau": [], "desc_quyenloi": []}
    current = "desc_mota"
    body = frag_soup.find("body") or frag_soup
    for el in body.find_all(recursive=False):
        el_text = text(el) or ""
        if len(el_text) < 50:
            bucket_key = _match_description_label(el_text.strip().lower())
            if bucket_key and bucket_key in buckets:
                current = bucket_key
                continue
        if el_text:
            buckets[current].append(el_text)
    return {key: " ".join(values) if values else None for key, values in buckets.items()}


def extract_json_ld_job_posting(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    """Parse a job posting's JSON-LD script into desc_mota/desc_yeucau/desc_quyenloi/deadline/company_industry."""
    result: Dict[str, Optional[str]] = {
        "desc_mota": None,
        "desc_yeucau": None,
        "desc_quyenloi": None,
        "deadline": None,
        "company_industry": None,
    }
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        candidates = data if isinstance(data, list) else [data]
        for entry in candidates:
            if not isinstance(entry, dict):
                continue
            entry_type = entry.get("@type")
            type_matches = entry_type == "JobPosting" or (
                isinstance(entry_type, list) and "JobPosting" in entry_type
            )
            if not type_matches:
                continue
            description_html = entry.get("description")
            if description_html and isinstance(description_html, str) and not result["desc_mota"]:
                split_result = _split_description_fragment(description_html)
                for key in ("desc_mota", "desc_yeucau", "desc_quyenloi"):
                    if split_result.get(key):
                        result[key] = split_result[key]
            valid_through = entry.get("validThrough")
            if valid_through and isinstance(valid_through, str) and not result["deadline"]:
                date_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", valid_through)
                if date_match:
                    year, month, day = date_match.groups()
                    result["deadline"] = f"{day}/{month}/{year}"
            industry = entry.get("industry")
            if industry and isinstance(industry, str) and not result["company_industry"]:
                result["company_industry"] = industry
            if result["desc_mota"] and result["deadline"] and result["company_industry"]:
                return result
    return result


def extract_deadline(value: Optional[str]) -> Optional[str]:
    """Pull a DD/MM/YYYY date out of a raw deadline string."""
    if not value:
        return None
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", value)
    return match.group(1) if match else value

DEBUG_DUMP_MISSING_DESC = os.getenv("DEBUG_DUMP_MISSING_DESC", "").strip().lower() in {"1", "true", "yes"}
DEBUG_DUMP_MAX_FILES = 5
_debug_dump_counts: Dict[str, int] = {}


def _debug_dump_html(url: str, html: str, kind: str = "desc_mota") -> None:
    """Dump raw HTML to disk for manual diagnosis when a field extraction fails (opt-in, capped file count)."""
    if not DEBUG_DUMP_MISSING_DESC or _debug_dump_counts.get(kind, 0) >= DEBUG_DUMP_MAX_FILES:
        return
    try:
        project_dir = _resolve_project_dir()
        dump_dir = os.path.join(project_dir, "data", "debug")
        os.makedirs(dump_dir, exist_ok=True)
        id_match = re.search(r"/(\d+)\.html", url)
        page_id = id_match.group(1) if id_match else str(_debug_dump_counts.get(kind, 0))
        dump_path = os.path.join(dump_dir, f"missing_{kind}_{page_id}.html")
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write(html)
        _debug_dump_counts[kind] = _debug_dump_counts.get(kind, 0) + 1
        logger.info("Dumped raw HTML for diagnosis: %s (url=%s)", dump_path, url)
    except Exception as exc:
        logger.warning("Failed to dump debug HTML for %s: %s", url, str(exc)[:200])


def _normalize_topcv_url(url: Optional[str]) -> Optional[str]:
    """Canonicalize a TopCV URL by stripping query params and fragments."""
    if not url:
        return url
    parsed = urlparse(url)
    return parsed._replace(query="", fragment="").geturl()


def scrape_job_detail(session: requests.Session, job_url: str) -> Dict:
    """Fetch and parse a job detail page into a flat field dict."""
    html = get_html(session, job_url)
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    company_href = apply_map(soup, FIELD_MAP["company_url_from_job"])
    company_url_from_job = _normalize_topcv_url(urljoin(BASE, company_href)) if company_href else None
    detail: Dict[str, Optional[str]] = {
        "title": apply_map(soup, FIELD_MAP["title"]),
        "salary": get_value_by_label(soup, ["mức lương", "thu nhập"]),
        "location": get_value_by_label(soup, ["địa điểm"]),
        "experience": get_value_by_label(soup, ["kinh nghiệm"]),
        "deadline": extract_deadline(
            get_value_by_label(soup, ["hạn ứng tuyển", "hạn nộp hồ sơ", "hạn nộp"])
            or apply_map(soup, FIELD_MAP["deadline"])
        ),
        "tags": apply_map(soup, FIELD_MAP["tags"], multi=True),
        "company_url_from_job": company_url_from_job,
        "company_name_full": apply_map(soup, FIELD_MAP["company_name_full"]),
        "company_website": apply_map(soup, FIELD_MAP["company_website"]),
        "company_size": _clean_short_field(apply_map(soup, FIELD_MAP["company_size"])),
        "company_followers": _clean_short_field(apply_map(soup, FIELD_MAP["company_followers"])),
        "company_industry": apply_map(soup, FIELD_MAP["company_industry"]),
        "company_address": apply_map(soup, FIELD_MAP["company_address"]),
    }

    sections = extract_description_sections(soup)
    detail.update(sections)

    json_ld = extract_json_ld_job_posting(soup)
    for key in ("desc_mota", "desc_yeucau", "desc_quyenloi"):
        json_ld_value = json_ld.get(key)
        if json_ld_value and len(json_ld_value) > len(detail.get(key) or ""):
            detail[key] = json_ld_value
    if not detail.get("deadline") and json_ld.get("deadline"):
        detail["deadline"] = json_ld["deadline"]
    if not detail.get("company_industry") and json_ld.get("company_industry"):
        detail["company_industry"] = json_ld["company_industry"]

    if not detail.get("desc_mota"):
        _debug_dump_html(job_url, html)

    detail["title"] = _normalize_job_title(detail.get("title"))
    return detail


def _extract_company_value_from_label_rows(soup: BeautifulSoup, labels: List[str]) -> Optional[str]:
    """Find a company field value from single-line 'label: value' style rows."""
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


def _find_section_heading(soup: BeautifulSoup, label_variants: List[str]) -> Optional[Any]:
    """Find the first h1/h2/h3 whose text matches one of the given labels."""
    label_norms = [re.sub(r"\s+", " ", l).strip().lower() for l in label_variants]
    for heading in soup.select("h1, h2, h3"):
        h_norm = re.sub(r"\s+", " ", text(heading) or "").strip().lower()
        if any(l in h_norm for l in label_norms):
            return heading
    return None


def _extract_text_after_heading(heading: Any, stop_tags: Tuple[str, ...] = ("h1", "h2")) -> Optional[str]:
    """Return the first non-empty text node after a heading, stopping at any of the given stop_tags."""
    for el in heading.find_all_next(True):
        if el.name in stop_tags:
            break
        if el.find(True) is not None:
            continue
        el_text = text(el)
        if el_text and len(el_text) > 5:
            return el_text
    return None


def _extract_value_after_label_element(
    soup: BeautifulSoup, labels: List[str], scope_heading: Optional[Any] = None
) -> Optional[str]:
    """Find a company field value by looking for a label element and returning the next non-empty text node."""
    label_norms = [re.sub(r"\s+", " ", l).strip().lower() for l in labels]
    if scope_heading is not None:
        candidates = scope_heading.find_all_next(True, limit=80)
    else:
        candidates = soup.find_all(True)
    for i, el in enumerate(candidates):
        el_text = re.sub(r"\s+", " ", text(el) or "").strip().lower()
        if el_text and el_text in label_norms:
            for nxt in candidates[i + 1 : i + 6]:
                val = re.sub(r"\s+", " ", text(nxt) or "").strip()
                if val and val.lower() not in label_norms:
                    return val
    return None


def _clean_short_field(value: Optional[str], max_len: int = 100) -> Optional[str]:
    """Return None if the field is empty or exceeds max_len, otherwise return the value."""
    if value is None:
        return None
    if len(value) > max_len:
        return None
    return value


def _is_noise_company_description(value: Optional[str]) -> bool:
    """Detect placeholder/error text that isn't a real company description."""
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
    """Fetch and parse a company page into a flat field dict."""
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

    info_heading = _find_section_heading(soup, ["thông tin chung", "thong tin chung"])

    size = _clean_short_field(apply_map(soup, FIELD_MAP["company_size"]))
    if not size:
        size = _clean_short_field(_extract_company_value_from_label_rows(soup, ["quy mô", "nhân viên", "size"]))
    if not size:
        size = _clean_short_field(
            _extract_value_after_label_element(soup, ["quy mô"], scope_heading=info_heading)
        )

    followers = _clean_short_field(apply_map(soup, FIELD_MAP["company_followers"]))
    if not followers:
        followers_match = re.search(r"\d[\d,\.]*\s*người theo dõi", soup.get_text(" ", strip=True), flags=re.I)
        if followers_match:
            followers = _clean_short_field(followers_match.group(0))

    industry = apply_map(soup, FIELD_MAP["company_industry"])
    if not industry:
        industry = _extract_company_value_from_label_rows(soup, ["lĩnh vực", "ngành nghề", "industry"])
    if not industry:
        industry = _extract_value_after_label_element(
            soup, ["lĩnh vực hoạt động", "lĩnh vực chính"], scope_heading=info_heading
        )

    address = apply_map(soup, FIELD_MAP["company_address"])
    if not address:
        address = _extract_company_value_from_label_rows(soup, ["địa chỉ", "address"])
    if not address:
        addr_heading = _find_section_heading(soup, ["địa điểm công ty", "địa chỉ công ty"])
        if addr_heading is not None:
            address = _extract_text_after_heading(addr_heading)

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

    if not address:
        _debug_dump_html(company_url, html, kind="company_address")

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