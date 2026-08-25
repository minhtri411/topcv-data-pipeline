import logging
import random
import time
from typing import Optional
from urllib.robotparser import RobotFileParser

import requests

from config import BASE, MAX_RETRIES, REQUEST_HEADERS, REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


def smart_sleep(min_s: float = 1.5, max_s: float = 3.0) -> None:
    """Sleep a random duration to stay polite between requests."""
    time.sleep(random.uniform(min_s, max_s))


_robots_parser: Optional["RobotFileParser"] = None
_robots_checked = False


def _load_robots_parser(session: requests.Session) -> Optional["RobotFileParser"]:
    """Fetch and cache robots.txt once per process (short retry on transient failure)."""
    global _robots_parser, _robots_checked
    if _robots_checked:
        return _robots_parser
    for attempt in range(1, 3):
        try:
            resp = session.get(f"{BASE}/robots.txt", timeout=REQUEST_TIMEOUT_SECONDS)
            if resp.status_code == 200 and resp.text:
                parser = RobotFileParser()
                parser.parse(resp.text.splitlines())
                _robots_parser = parser
                logger.info("Loaded robots.txt from %s/robots.txt", BASE)
                break
            logger.warning("robots.txt fetch returned status %s (attempt %s)", resp.status_code, attempt)
        except Exception as exc:
            logger.warning("Failed to load robots.txt (attempt %s): %s", attempt, str(exc)[:200])
        if attempt < 2:
            time.sleep(random.uniform(3, 6))
    _robots_checked = True
    if _robots_parser is None:
        logger.error("Could not verify robots.txt after retries — crawl will skip ALL URLs this run")
    return _robots_parser


def is_allowed_by_robots(session: requests.Session, url: str) -> bool:
    """Check robots.txt permission for a URL; fail-closed if it can't be verified."""
    parser = _load_robots_parser(session)
    if parser is None:
        # Fail-closed: if we cannot verify permission, do not crawl.
        logger.warning("Cannot verify robots.txt; skipping URL: %s", url)
        return False
    return parser.can_fetch("*", url)


def build_session() -> requests.Session:
    """Create a requests.Session with browser-like headers."""
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    return session


def warmup_session(session: requests.Session) -> None:
    """Prime cookies by visiting the homepage before real crawling starts."""
    try:
        session.get(BASE, timeout=REQUEST_TIMEOUT_SECONDS)
        smart_sleep(0.8, 1.5)
    except Exception as exc:
        logger.warning("Warmup failed: %s", str(exc)[:200])


def get_html(session: requests.Session, url: str) -> Optional[str]:
    """Fetch a URL's HTML, checking robots.txt first and retrying with backoff on block/rate-limit."""
    if not is_allowed_by_robots(session, url):
        logger.warning("Blocked by robots.txt, skipping: %s", url)
        return None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code in (403, 429) or "<title>Just a moment...</title>" in response.text:
                logger.warning("Blocked/rate-limited at URL: %s (attempt %s)", url, attempt)
                if attempt == MAX_RETRIES:
                    logger.warning("Giving up on %s after %s attempt(s)", url, MAX_RETRIES)
                    return None
                backoff = random.uniform(5, 10) * attempt
                logger.info("Backing off for %.1fs before retrying (no bypass attempted)...", backoff)
                time.sleep(backoff)
                continue
            response.raise_for_status()
            return response.text
        except Exception as exc:
            logger.warning("Fetch failed at URL: %s (attempt %s): %s", url, attempt, str(exc)[:200])
            if attempt == MAX_RETRIES:
                return None
            time.sleep(random.uniform(5, 10))
    return None