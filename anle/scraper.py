#!/usr/bin/env python3
"""
Scraper for https://anle.toaan.gov.vn/webcenter/portal/anle/nguonanle

Downloads all PDFs (court case source documents) and saves metadata to CSV + JSON.
Each "item" processed by this scraper is a listing page; resume is tracked at
page granularity.

Usage:
    python scraper.py                           # scrape all pages
    python scraper.py --start 1 --end 100       # scrape pages 1-100
    python scraper.py --num-workers 4           # 4 parallel PDF download workers
    python scraper.py --no-pdf                  # metadata only, skip PDFs
    python scraper.py --output ./anle_data      # custom output directory
    python scraper.py --no-resume               # ignore previous progress
"""

import argparse
import csv
import json
import logging
import re
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://anle.toaan.gov.vn"
LIST_URL = f"{BASE_URL}/webcenter/portal/anle/nguonanle"
DETAIL_URL = f"{BASE_URL}/webcenter/portal/anle/chitietnguonanle"
PDF_URL = f"{BASE_URL}/webcenter/ShowProperty"

ITEMS_PER_PAGE = 10
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


@dataclass
class CaseRecord:
    stt: str = ""
    doc_name: str = ""
    title: str = ""
    date: str = ""
    summary: str = ""
    court: str = ""
    pdf_url: str = ""
    detail_url: str = ""
    pdf_filename: str = ""
    page: int = 0


_BLOCK_SIGNATURES = (
    "UNEXPECTED_EOF_WHILE_READING",
    "SSLEOFError",
    "SSLError",
    "Connection reset by peer",
    "ConnectionResetError",
    "Connection aborted",
    "RemoteDisconnected",
    "ECONNRESET",
)


def _looks_like_network_block(exc: BaseException) -> bool:
    """Heuristic: did the TLS/TCP handshake get closed before any HTTP data?

    These signatures come up when a host geo-blocks the source IP or when a
    middlebox is silently dropping/intercepting the connection. They're
    indistinguishable at the socket layer but the user-facing fix is the same:
    route through a reachable egress (VN VPS / VN proxy / corporate proxy).
    """
    s = repr(exc)
    return any(sig in s for sig in _BLOCK_SIGNATURES)


def make_session() -> requests.Session:
    session = requests.Session()
    session.verify = False
    # Oracle ADF returns a JS-based loopback page for browser-like Accept headers.
    # Using a simple Accept: */* bypasses the ADF session init and returns full HTML.
    session.headers.update({
        "User-Agent": "anle-scraper/1.0",
        "Accept": "*/*",
    })
    adapter = requests.adapters.HTTPAdapter(
        max_retries=urllib3.Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_listing_page(html_text: str, page: int) -> list[CaseRecord]:
    """Parse a listing page HTML and extract case records.

    The table is rendered server-side with single-quoted attributes; lxml can
    struggle with those inside deeply nested ADF markup, so we first extract
    the table via regex, then parse it with BS4.
    """
    records: list[CaseRecord] = []

    table_match = re.search(
        r"<table\s+class='table\s+table-bordered[^']*'>(.+?)</table>",
        html_text,
        re.DOTALL,
    )
    if not table_match:
        return records

    table_html = f"<table>{table_match.group(1)}</table>"
    soup = BeautifulSoup(table_html, "lxml")
    rows = soup.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        record = CaseRecord(page=page)
        record.stt = cells[0].get_text(strip=True)

        title_link = cells[1].find("a")
        if title_link:
            record.title = title_link.get_text(strip=True)
            href = title_link.get("href", "")
            doc_match = re.search(r"dDocName=(\w+)", href)
            if doc_match:
                record.doc_name = doc_match.group(1)
                record.detail_url = f"{DETAIL_URL}?dDocName={record.doc_name}"
                record.pdf_url = f"{PDF_URL}?nodeId=/UCMServer/{record.doc_name}"
                record.pdf_filename = f"{record.doc_name}.pdf"

        record.date = cells[2].get_text(strip=True)

        summary_link = cells[3].find("a")
        if summary_link:
            record.summary = summary_link.get_text(strip=True)
        else:
            record.summary = cells[3].get_text(strip=True)

        if len(cells) >= 5:
            record.court = cells[4].get_text(strip=True)

        if record.doc_name:
            records.append(record)

    return records


class AnleScraper:
    """Scraper for anle.toaan.gov.vn. One 'item' == one listing page."""

    CSV_FIELDS = [
        "stt", "doc_name", "title", "date", "summary",
        "court", "pdf_url", "detail_url", "pdf_filename", "page",
    ]

    def __init__(
        self,
        output_dir: str,
        num_workers: int = 2,
        delay: float = 1.0,
        download_pdfs: bool = True,
        proxy: Optional[str] = None,
    ):
        self.output_dir = Path(output_dir)
        self.pdf_dir = self.output_dir / "pdfs"
        self.num_workers = num_workers
        self.delay = delay
        self.download_pdfs = download_pdfs

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)

        self.session = make_session()
        # anle.toaan.gov.vn is usually globally reachable, but some networks
        # (or the sibling congbobanan host) may require a VN-based route.
        # --proxy takes precedence over HTTP_PROXY/HTTPS_PROXY env vars.
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

        self.progress_file = self.output_dir / "progress.json"
        self.csv_file = self.output_dir / "data.csv"
        self.json_file = self.output_dir / "data.json"

        # Backwards-compat: pre-refactor file names were metadata.{csv,json}.
        # If a fresh run finds the legacy names (but not the new ones), use
        # them so resume keeps working.
        legacy_csv = self.output_dir / "metadata.csv"
        legacy_json = self.output_dir / "metadata.json"
        if legacy_csv.exists() and not self.csv_file.exists():
            legacy_csv.rename(self.csv_file)
        if legacy_json.exists() and not self.json_file.exists():
            legacy_json.rename(self.json_file)

    # ----- networking -----

    def fetch_page(self, page: int) -> Optional[str]:
        params = {
            "selectedPage": str(page),
            "docType": "NguonAnLe",
            "mucHienThi": "9015",
        }
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(LIST_URL, params=params, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                log.warning("Page %d attempt %d failed: %s", page, attempt + 1, e)
                if _looks_like_network_block(e):
                    log.error(
                        "TLS/connection closed before any data arrived - the "
                        "host is likely geo-blocking your source IP or a "
                        "firewall is intercepting the handshake. Run on a "
                        "VN-based VPS, set --proxy (or HTTPS_PROXY) to a VN "
                        "exit, or configure your corporate HTTPS proxy."
                    )
                    return None
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
        return None

    def detect_last_page(self) -> int:
        """Binary search for the last valid page number.

        The site wraps back to page-1 content when you request beyond the last
        page, so we compare each probe against the first page's doc IDs.
        """
        log.info("Detecting total number of pages...")

        first_html = self.fetch_page(1)
        if not first_html:
            log.error("Cannot fetch page 1 - defaulting to 100 pages")
            return 100
        first_records = parse_listing_page(first_html, 1)
        if not first_records:
            log.error("Cannot parse page 1 - defaulting to 100 pages")
            return 100
        first_doc = first_records[0].doc_name

        def page_has_unique_content(page: int) -> bool:
            html_text = self.fetch_page(page)
            if not html_text:
                return False
            records = parse_listing_page(html_text, page)
            if not records:
                return False
            return records[0].doc_name != first_doc

        lo, hi = 1, 10000
        for probe in [100, 500, 1000, 2000, 4000, 6000, 8000, 10000]:
            log.info("  probing page %d...", probe)
            if not page_has_unique_content(probe):
                hi = probe
                break
            lo = probe

        while lo < hi - 1:
            mid = (lo + hi) // 2
            log.info("  binary search: checking page %d (range %d-%d)", mid, lo, hi)
            if page_has_unique_content(mid):
                lo = mid
            else:
                hi = mid

        log.info("Detected last page: %d (approx. %d entries)", lo, lo * ITEMS_PER_PAGE)
        return lo

    def download_pdf(self, record: CaseRecord) -> bool:
        """Download a single PDF. Returns True on success (or if already on disk)."""
        pdf_path = self.pdf_dir / record.pdf_filename
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            return True

        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(record.pdf_url, timeout=REQUEST_TIMEOUT, stream=True)
                resp.raise_for_status()

                content_type = resp.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    log.warning("%s: got HTML instead of PDF, skipping", record.doc_name)
                    return False

                tmp_path = pdf_path.with_suffix(".tmp")
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)

                if tmp_path.stat().st_size == 0:
                    tmp_path.unlink()
                    log.warning("%s: downloaded 0 bytes", record.doc_name)
                    return False

                tmp_path.rename(pdf_path)
                return True

            except requests.RequestException as e:
                log.warning("%s attempt %d failed: %s", record.doc_name, attempt + 1, e)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))

        return False

    # ----- metadata persistence -----

    def save_metadata(self, records: list[CaseRecord]):
        """Append new records to CSV and rewrite the full JSON."""
        existing_docs: set[str] = set()
        if self.csv_file.exists():
            with open(self.csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_docs.add(row.get("doc_name", ""))

        new_records = [r for r in records if r.doc_name not in existing_docs]
        if not new_records:
            return

        write_header = not self.csv_file.exists()
        with open(self.csv_file, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
            if write_header:
                writer.writeheader()
            for record in new_records:
                writer.writerow(asdict(record))

        all_records: list[dict] = []
        if self.json_file.exists():
            with open(self.json_file, "r", encoding="utf-8") as f:
                all_records = json.load(f)
        all_records.extend([asdict(r) for r in new_records])
        with open(self.json_file, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)

    # ----- progress / resume -----

    def load_progress(self) -> dict:
        """Return {completed: set[int], last_id: int, page_docs: dict[str, list[str]]}."""
        if self.progress_file.exists():
            data = json.loads(self.progress_file.read_text())
            return {
                "completed": set(data.get("completed", [])),
                "last_id": int(data.get("last_id", 0)),
                "page_docs": data.get("page_docs", {}),
            }

        # Backwards-compat: migrate the pre-refactor ``.crawl_state.json``
        # format ({"crawled_pages": [...]}) into the new progress schema.
        legacy = self.output_dir / ".crawl_state.json"
        if legacy.exists():
            data = json.loads(legacy.read_text())
            pages = [int(p) for p in data.get("crawled_pages", [])]
            completed = set(pages)
            last_id = max(pages) if pages else 0
            log.info("Migrated legacy progress file: %d pages", len(pages))
            return {"completed": completed, "last_id": last_id, "page_docs": {}}

        return {"completed": set(), "last_id": 0, "page_docs": {}}

    def save_progress(self, completed: set[int], last_id: int, page_docs: dict[str, list[str]]):
        self.progress_file.write_text(json.dumps({
            "completed": sorted(completed),
            "last_id": last_id,
            "count": len(completed),
            "page_docs": page_docs,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        }, ensure_ascii=False, indent=2))

    def is_item_complete(self, page_id: int, progress: dict) -> bool:
        """A page is complete iff it was checkpointed AND every PDF we recorded
        for it still exists on disk. This catches the 'partial download' case
        where progress.json says done but the user deleted some PDFs (or a
        previous run crashed mid-page before flushing the checkpoint).
        """
        if page_id not in progress["completed"]:
            return False
        if not self.download_pdfs:
            return True
        doc_names = progress["page_docs"].get(str(page_id), [])
        if not doc_names:
            return True
        for doc in doc_names:
            pdf = self.pdf_dir / f"{doc}.pdf"
            if not (pdf.exists() and pdf.stat().st_size > 0):
                return False
        return True

    # ----- per-item worker -----

    def process_item(self, page_id: int) -> Optional[list[str]]:
        """Process one page. Returns the list of doc_names for that page on
        success, or None if the page could not be fetched/parsed.
        """
        html_text = self.fetch_page(page_id)
        if not html_text:
            log.error("Failed to fetch page %d", page_id)
            return None

        records = parse_listing_page(html_text, page_id)
        if not records:
            log.warning("No records found on page %d", page_id)
            return []

        log.info("Page %d: found %d records", page_id, len(records))
        self.save_metadata(records)

        if self.download_pdfs:
            failed: list[str] = []
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                futures = {executor.submit(self.download_pdf, r): r for r in records}
                for future in as_completed(futures):
                    record = futures[future]
                    try:
                        if not future.result():
                            failed.append(record.doc_name)
                    except Exception as e:
                        log.error("PDF download error for %s: %s", record.doc_name, e)
                        failed.append(record.doc_name)
            if failed:
                log.warning("Page %d: %d/%d PDFs failed", page_id, len(failed), len(records))

        return [r.doc_name for r in records]

    # ----- main loop -----

    def run(self, start_id: int, end_id: Optional[int], resume: bool = True):
        if end_id is None:
            end_id = self.detect_last_page()

        progress = self.load_progress() if resume else {
            "completed": set(), "last_id": 0, "page_docs": {}
        }

        # Resume from the page right after the last fully-completed one,
        # but never before the user-specified start.
        resume_from = max(start_id, progress["last_id"] + 1) if progress["last_id"] else start_id
        log.info(
            "Scraping pages %d-%d (resuming from %d; %d pages already done)",
            start_id, end_id, resume_from, len(progress["completed"]),
        )

        completed: set[int] = progress["completed"]
        page_docs: dict[str, list[str]] = progress["page_docs"]
        last_id = progress["last_id"]
        total_records = 0
        failed_pages: list[int] = []

        for page_id in range(resume_from, end_id + 1):
            if self.is_item_complete(page_id, progress):
                log.debug("Page %d already complete, skipping", page_id)
                last_id = max(last_id, page_id)
                continue

            log.info("--- Page %d / %d ---", page_id, end_id)
            doc_names = self.process_item(page_id)
            if doc_names is None:
                failed_pages.append(page_id)
                continue

            total_records += len(doc_names)
            completed.add(page_id)
            page_docs[str(page_id)] = doc_names
            last_id = max(last_id, page_id)

            progress = {"completed": completed, "last_id": last_id, "page_docs": page_docs}
            self.save_progress(completed, last_id, page_docs)

            if page_id < end_id:
                time.sleep(self.delay)

        log.info("=" * 60)
        log.info(
            "Done. Pages completed: %d | new records: %d | failed pages: %d",
            len(completed), total_records, len(failed_pages),
        )
        if failed_pages:
            log.warning("Failed pages: %s", failed_pages[:20])
            (self.output_dir / "failed_pages.json").write_text(
                json.dumps(failed_pages, indent=2)
            )


def main():
    parser = argparse.ArgumentParser(
        description="Scrape court case PDFs from anle.toaan.gov.vn"
    )
    parser.add_argument(
        "--start", type=int, default=1,
        help="First page number to scrape (default: 1)",
    )
    parser.add_argument(
        "--end", type=int, default=None,
        help="Last page number to scrape (default: auto-detect)",
    )
    parser.add_argument(
        "--num-workers", type=int, default=2,
        help="Parallel PDF download workers (default: 2)",
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Delay in seconds between page requests (default: 1.0)",
    )
    parser.add_argument(
        "--output", type=str, default="./data",
        help="Output directory (default: ./data)",
    )
    parser.add_argument(
        "--no-pdf", action="store_true",
        help="Only download metadata, skip PDFs",
    )
    parser.add_argument(
        "--no-resume", action="store_true",
        help="Don't resume from previous progress",
    )
    parser.add_argument(
        "--proxy", type=str, default=None,
        help="HTTP/HTTPS/SOCKS proxy URL (e.g. http://user:pass@host:port, "
             "socks5h://host:port). HTTP_PROXY/HTTPS_PROXY env vars are also honored.",
    )
    args = parser.parse_args()

    scraper = AnleScraper(
        output_dir=args.output,
        num_workers=args.num_workers,
        delay=args.delay,
        download_pdfs=not args.no_pdf,
        proxy=args.proxy,
    )
    scraper.run(
        start_id=args.start,
        end_id=args.end,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    main()
