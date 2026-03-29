import random
import time
import re
import random
import traceback
import traceback
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
from urllib3.util import Retry

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import logging

import pandas as pd
import os
import sys
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE = "https://www.topcv.vn"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/123.0.0.0 Safari/537.36"),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.topcv.vn/",
    "Connection": "keep-alive",
}

# Extract text from an element
def text(el) -> Optional[str]:
    if not el:
        return None
    
    t = el.get_text(" ", strip=True)
    return re.sub(r"\s+"," ",t) if t else None

def smart_sleep(min_seconds=1.2, max_seconds=3):
    time.sleep(random.uniform(min_seconds, max_seconds))

def build_session() -> requests.Session:
    '''Build a session with headers to keep the connection alive and avoid being blocked by the server'''
    s = requests.Session()
    s.headers.update(HEADERS)

    # Retry adapter (network-level)
    retry = Retry(
        total=6,
        connect =3,
        read=3,
        status=6,
        backoff_factor=1.2,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    ) 

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    # Warm up the session by making an initial request to the base URL, which can help establish a connection and reduce the chances of being blocked on subsequent requests.
    try:
        s.get(BASE, timeout=20)
        time.sleep(1)
    except requests.exceptions.RequestException:
        pass

    return s


def get_soup(session, url) -> BeautifulSoup:
    '''
    Reuse the session to get the soup object for a given URL, which allows us to parse the HTML content
    (request -> check 429 -> retry -> parse HTML)
    '''
    # Retry (Application-level)
    for attempt in range(1,6):
        r = session.get(url, timeout=30)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = int(retry_after)
                except ValueError:
                    wait = 6 * attempt
            else:
                wait = 6 * attempt
            wait = wait + random.uniform(0.5,2)
            logger.warning(f"Received 429 Too Many Requests. Waiting for {wait:.2f} seconds before retrying...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    r.raise_for_status()  # If we exhaust retries, raise the last exception
    return BeautifulSoup("", "lxml")  # Return empty soup if all retries fail



# Step 2: Crawl search page
def parse_search_page(session, url):
    '''
    Parse the search results page to extract job posting URLs and their corresponding job titles.
    This function will return a list of tuples (job_title, job_url) for each job posting found on the search results page.
    '''
    soup = get_soup(session, url)
    jobs = []
    for job in soup.select("div.job-item-search-result"):
        a_title = job.select_one("h3.title a[href]")

        if not a_title:
            continue

        title = text(a_title)
        job_url = urljoin(BASE, a_title.get("href"))
        a_comp = job.select_one("a.company[href]")
        company = text(job.select_one("a.company .company-name"))
        company_url = urljoin(BASE, a_comp.get("href")) if a_comp else None
        salary = text(job.select_one("label.title-salary"))
        address = text(job.select_one("label.address .city-text"))
        exp = text(job.select_one("label.exp span"))
        jobs.append({
            "title": title,
            "job_url": job_url,
            "company": company,
            "company_url": company_url,
            "salary": salary,
            "address_list": address,
            "exp_list": exp,
        })
    return jobs
    
def pick_info_value(soup, title):
    for sec in soup.select(".job-detail__info--section"):
        t = text(sec.select_one(".job-detail__info--section-content-title"))
        if t.lower() == title.lower():
            v = sec.select_one(".job-detail__info--section-content-value")
            return text(v) if v else text(sec)
        
    return None

def extract_deadline(soup):
    for el in soup.select(".job-detail__info--deadline, .job-detail__information-detail--actions-label"):
        t = text(el)
        if t and "Hạn nộp" in t:
            m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", t)
            return m.group(1) if m else t
    return None

def extract_tags(soup):
    return [text(t) for t in soup.select(".job-tags a.item") if text(t)]

def extract_desc_block(soup):
    data={}
    for el in soup.select(".job-description .job-description__item"):
        title = text(el.select_one("h3")) or ""
        content = el.select_one(".job-description__item--content")
        if content:
            data[title] = text(content)
    return data

def extract_section_list(soup, title):
    out = []
    for el in soup.select(".job-description__item h3"):
        if title in (text(el) or ""):
            wrap = el.find_parent(class_="job-description__item")
            if wrap:
                for d in wrap.select(
                    ".job-description__item--content div, "
                    ".job-description__item--content li"
                ):
                    val = text(d)
                    if val:
                        out.append(val)
    return out

def extract_company_link_from_job(soup):
    cand = soup.select_one("a.company[href]") or soup.select_one("a[href*='/cong-ty/']")
    return urljoin(BASE, cand["href"]) if cand and cand.has_attr("href") else None

def scrape_job_details(session, job_url):
    '''
    Scrape the job details from the job posting page, including job title, company name, location, salary, and job description.
    This function will return a dictionary containing the extracted information for the given job URL.
    (open job page -> parse fields -> return dict)
    '''
    soup = get_soup(session, job_url)
    smart_sleep()
    title = text(soup.select_one(".job-detail__info--title, h1"))
    salary = pick_info_value(soup, "Mức lương")
    location = pick_info_value(soup, "Địa điểm")
    experience = pick_info_value(soup, "Kinh nghiệm")
    deadline = extract_deadline(soup)
    tags = extract_tags(soup)
    desc_block = extract_desc_block(soup)
    addrs = extract_section_list(soup, "Địa điểm làm việc")
    times = extract_section_list(soup, "Thời gian làm việc")
    company_url_detail = extract_company_link_from_job(soup)

    return {
        "title": title,
        "detail_salary": salary,
        "location": location,
        "experience": experience,
        "deadline": deadline,
        "tags": "; ".join(tags) if tags else None,
        "desc_mota": desc_block.get("Mô tả công việc"),
        "desc_yeucau": desc_block.get("Yêu cầu ứng viên"),
        "desc_quyenloi": desc_block.get("Quyền lợi"),
        "working_addresses": "; ".join(addrs) if addrs else None,
        "working_times": "; ".join(times) if times else None,
        "company_url_from_job": company_url_detail,
    }


def scraper_company(session, company_url):

    if not company_url:
        return {
            "company_name_full": None,
            "company_website": None,
            "company_size": None,
            "company_followers": None,
            "company_address": None,
            "company_description": None,
        }

    soup = get_soup(session, company_url)
    smart_sleep()

    company_name = text(soup.select_one("h1.company-detail-name"))

    a_website = soup.select_one("a.company-subdetail-info-text[href^='http']")
    website = a_website.get("href") if a_website else None

    size = None
    followers = None
    industry = None

    for el in soup.select(".company-subdetail-info"):
        v = text(el.select_one(".company-subdetail-info-text"))
        if not v:
            continue
        v_lower = v.lower()
        if "nhân viên" in v_lower:
            size = v
        elif "người theo dõi" in v_lower:
            followers = v
        elif "lĩnh vực" in v_lower:
            industry = v

    address = text(soup.select_one(".desc"))
    description = text(soup.select_one(".content"))

    return {
        "company_name_full": company_name,
        "company_website": website,
        "company_size": size,
        "company_followers": followers,
        "company_industry": industry,
        "company_address": address,
        "company_description": description,
    }
                           
    

# search page -> job details -> company details
def crawl_to_dataframe(
        query_url_template: str,
        start_page: int = 1,
        end_page: int = 1,
        delay_between_pages=(0.5,1)
) -> pd.DataFrame:
    rows: List[Dict] = []
    seen_jobs = set()
    company_cache = {}

    s = build_session()
    for page in range(start_page, end_page + 1):
        url = query_url_template.format(page=page)
        logger.info(f"Processing search results page {page}: {url}")
        jobs = parse_search_page(s, url)

        if not jobs:
            logger.warning(f"No jobs found on page {page}. Stopping pagination.")
            break

        for job in jobs:
            job_url = job["job_url"]
            job_id = urlparse(job_url).path
            if job_id in seen_jobs:
                continue
            seen_jobs.add(job_id)
            try:
                detail = scrape_job_details(s, job_url)
            except Exception as e:
                logger.error(f"Error occurred while scraping job details for {job_url}: {e}")
                detail = {
                    "title": None,
                    "detail_salary": None,
                    "location": None,
                    "experience": None,
                    "deadline": None,
                    "tags": None,
                    "desc_mota": None,
                    "desc_yeucau": None,
                    "desc_quyenloi": None,
                    "working_addresses": None,
                    "working_times": None,
                    "company_url_from_job": None,
                }
            company_url = job.get("company_url") or detail.get("company_url_from_job")
            if company_url in company_cache:
                company_info = company_cache[company_url]
            else:
                try: 
                    company_info = scraper_company(s, company_url)
                    company_cache[company_url] = company_info
                except Exception as e:
                    logger.error(f"Error occurred while scraping company details for {company_url}: {e}")
                    company_info = {
                        "company_name_full": None,
                        "company_website": None,
                        "company_size": None,
                        "company_followers": None,
                        "company_industry": None,
                        "company_address": None,
                        "company_description": None,
                    }
            row = {**job, **detail, **company_info}
            rows.append(row)
        smart_sleep(*delay_between_pages)
    df = pd.DataFrame(rows)
    # Arrange cols
    cols = [
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
        "company_description",
    ]
    cols = [c for c in cols if c in df.columns]
    return df.loc[:, cols] if cols else df







def main():
    query_template = (
        "https://www.topcv.vn/tim-viec-lam-data?type_keyword=1&page={page}&sba=1"
    )
    logger.info("Starting crawling jobs...")
    start_page = 1
    end_page = 2

    try:
        df = crawl_to_dataframe(query_template, start_page, end_page)
    except Exception:
        logger.error("Crawler failed")
        logger.error(traceback.format_exc())
        sys.exit(1) 

    if df.empty:
        logger.warning("No data collected. Stop")
        return
    logger.info(f"Total jobs collected: {len(df)}")
    project_dir = os.environ.get("PROJECT_DIR", "/opt/project")
    output_dir = f"{project_dir}/data/raw"
    os.makedirs(output_dir, exist_ok = True)
    today = datetime.today().strftime("%Y%m%d")

    csv_path = f"{output_dir}/topcv_jobs_{today}.csv"
    df.to_csv(csv_path, index = False, encoding="utf-8-sig")
    logger.info(f"Save CSV: {csv_path}")
if __name__ == "__main__":
        main()