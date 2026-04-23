#!/usr/bin/env python3
"""
Scraper for https://congbobanan.toaan.gov.vn - Vietnamese Court Judgment Portal.

Downloads PDFs and metadata for all published court decisions. Each "item"
processed by this scraper is one case ID; resume is tracked at case-ID
granularity.

Usage:
    python scraper.py --start 1 --end 2100400 --num-workers 4 --output ./data
    python scraper.py --start 1000000 --end 1000100 --metadata-only
    python scraper.py --output ./data                     # resumes by default
    python scraper.py --no-resume --output ./data         # force restart
"""

import argparse
import csv
import json
import logging
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import unquote

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://congbobanan.toaan.gov.vn"
DETAIL_URL = BASE_URL + "/2ta{id}t1cvn/chi-tiet-ban-an"
PDF_URL = BASE_URL + "/3ta{id}t1cvn/"
DOWNLOAD_URL = BASE_URL + "/5ta{id}t1cvn/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

REQUEST_TIMEOUT = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


@dataclass
class CaseMetadata:
    id: int
    url: str = ""
    doc_type: str = ""           # "ban-an" (judgment) or "quyet-dinh" (decision)
    ban_an_so: str = ""          # Case number (e.g. "03/2022/DSST")
    ngay: str = ""               # Date (e.g. "23/11/2022")
    luot_xem: int = 0            # View count
    luot_tai: int = 0            # Download count
    ten_ban_an: str = ""         # Case name
    ngay_cong_bo: str = ""       # Publication date
    quan_he_phap_luat: str = ""  # Legal relationship
    cap_xet_xu: str = ""         # Court level
    loai_vu_viec: str = ""       # Case type
    toa_an_xet_xu: str = ""      # Court name
    ap_dung_an_le: str = ""      # Applied precedent
    dinh_chinh: str = ""         # Corrections
    thong_tin_vu_viec: str = ""  # Case info
    tong_binh_chon: str = ""     # Votes for precedent
    has_metadata: bool = False   # Whether the detail page had real metadata
    pdf_filename: str = ""       # Original PDF filename from download link
    pdf_saved_as: str = ""       # Actual filename saved on disk
    pdf_size_bytes: int = 0


# ----- string helpers -----

def sanitize_filename(name: str, max_len: int = 200) -> str:
    """Make a string safe for use as a filename on all platforms."""
    name = unicodedata.normalize("NFC", name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name)
    name = re.sub(r'[.\s]+$', '', name)
    name = re.sub(r'_+', '_', name).strip('_')
    if len(name) > max_len:
        name = name[:max_len].rstrip('_')
    return name


def format_case_number(raw: str) -> str:
    """Turn '03/2022/DSST' into '03-2022-DSST'."""
    return raw.replace("/", "-")


def format_date_yyyymmdd(raw: str) -> str:
    """Turn 'dd/mm/yyyy' into 'yyyymmdd'."""
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', raw)
    if m:
        return f"{m.group(3)}{m.group(2).zfill(2)}{m.group(1).zfill(2)}"
    return raw.replace("/", "")


def shorten_location(raw: str, max_len: int = 50) -> str:
    """Collapse whitespace/commas into hyphens and trim to max_len."""
    loc = re.sub(r'[\s,]+', '-', raw.strip()).strip('-')
    loc = re.sub(r'-+', '-', loc)
    if len(loc) > max_len:
        loc = loc[:max_len].rstrip('-')
    return loc


def build_pdf_name(meta: 'CaseMetadata') -> str:
    """Build a descriptive PDF filename from case metadata.

    Format: {id}_{type}_{typeid}_{yyyymmdd}_{category}_{location}.pdf
    Example: 1213296_ban-an_03-2022-DSST_20221123_Dan-su_TAND-tinh-Bac-Ninh.pdf
    """
    parts = [str(meta.id)]

    parts.append(meta.doc_type or "unknown")

    if meta.ban_an_so:
        parts.append(format_case_number(meta.ban_an_so))

    if meta.ngay:
        parts.append(format_date_yyyymmdd(meta.ngay))

    if meta.loai_vu_viec:
        parts.append(re.sub(r'\s+', '-', meta.loai_vu_viec.strip()))

    if meta.toa_an_xet_xu:
        parts.append(shorten_location(meta.toa_an_xet_xu))

    raw = "_".join(parts) + ".pdf"
    return sanitize_filename(raw)


# ----- HTML parsing helpers -----

def strip_tags(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def extract_between(html: str, after: str, before: str) -> str:
    idx = html.find(after)
    if idx == -1:
        return ""
    start = idx + len(after)
    end = html.find(before, start)
    if end == -1:
        return html[start:start + 500]
    return html[start:end]


def parse_label_span(html: str, label: str) -> str:
    pattern = re.compile(
        rf"<label[^>]*>\s*{re.escape(label)}\s*</label>\s*<span[^>]*>(.*?)</span>",
        re.DOTALL | re.IGNORECASE,
    )
    m = pattern.search(html)
    if m:
        return strip_tags(m.group(1)).strip()
    return ""


def page_has_metadata(html: str) -> bool:
    """Check whether the detail page actually contains the metadata panel.

    Some IDs return HTTP 200 but the page is a ghost record with no metadata
    sidebar - just the feedback form with heading 'null'.  Pages can be either
    "Bản án" (Judgment) or "Quyết định" (Decision).
    """
    has_case_number = ('Bản án số:' in html) or ('Quyết định số:' in html)
    has_sidebar = 'search_left_pub details_pub' in html
    return has_case_number and has_sidebar


def parse_metadata(case_id: int, html: str) -> CaseMetadata:
    meta = CaseMetadata(id=case_id)
    meta.url = DETAIL_URL.format(id=case_id)
    meta.has_metadata = page_has_metadata(html)

    if not meta.has_metadata:
        return meta

    panel = extract_between(html, 'class="panel panel-blue"', 'class="Detail_Feedback_pub"')
    if not panel:
        panel = html

    heading_match = re.search(
        r'<label>\s*(Bản án|Quyết định) số:\s*</label>\s*<span>(.*?)</span>',
        panel, re.DOTALL,
    )
    if heading_match:
        meta.doc_type = "ban-an" if "Bản án" in heading_match.group(1) else "quyet-dinh"
        raw = strip_tags(heading_match.group(2))
        parts = re.split(r'\s*ngày\s*', raw, maxsplit=1)
        meta.ban_an_so = parts[0].strip()
        if len(parts) > 1:
            meta.ngay = parts[1].strip()

    eye_match = re.search(r'fa-eye[^<]*</i>\s*([\d,.\s]+)', panel)
    if eye_match:
        meta.luot_xem = int(re.sub(r'\D', '', eye_match.group(1)))

    dl_match = re.search(r'fa-download[^<]*</i>\s*([\d,.\s]+)', panel)
    if dl_match:
        meta.luot_tai = int(re.sub(r'\D', '', dl_match.group(1)))

    ten_raw = parse_label_span(panel, "Tên bản án:")
    if not ten_raw:
        ten_raw = parse_label_span(panel, "Tên quyết định:")
    time_match = re.search(r'\((\d{2}\.\d{2}\.\d{4})\)', ten_raw)
    if time_match:
        meta.ngay_cong_bo = time_match.group(1)
        meta.ten_ban_an = ten_raw[:ten_raw.find("(")].strip()
    else:
        meta.ten_ban_an = ten_raw

    meta.quan_he_phap_luat = parse_label_span(panel, "Quan hệ pháp luật:")
    meta.cap_xet_xu = parse_label_span(panel, "Cấp xét xử:")
    meta.loai_vu_viec = parse_label_span(panel, "Loại vụ/việc:")
    meta.toa_an_xet_xu = parse_label_span(panel, "Tòa án xét xử:")
    meta.ap_dung_an_le = parse_label_span(panel, "Áp dụng án lệ:")
    meta.dinh_chinh = parse_label_span(panel, "Đính chính:")
    meta.thong_tin_vu_viec = parse_label_span(panel, "Thông tin về vụ/việc:")

    vote_match = re.search(
        r'Tổng số lượt được bình chọn làm nguồn phát triển án lệ:\s*([\d]+)',
        panel,
    )
    if vote_match:
        meta.tong_binh_chon = vote_match.group(1)

    pdf_link = re.search(
        rf'href="/5ta{case_id}t1cvn/([^"]+)"',
        html,
    )
    if pdf_link:
        meta.pdf_filename = unquote(pdf_link.group(1))

    return meta


class CongboScraper:
    """Scraper for congbobanan.toaan.gov.vn. One 'item' == one case ID."""

    def __init__(
        self,
        output_dir: str,
        num_workers: int = 4,
        delay: float = 0.3,
        timeout: int = REQUEST_TIMEOUT,
        metadata_only: bool = False,
        batch_size: int = 100,
        proxy: Optional[str] = None,
    ):
        self.output_dir = Path(output_dir)
        self.pdf_dir = self.output_dir / "pdfs"
        self.meta_dir = self.output_dir / "metadata"
        self.num_workers = num_workers
        self.delay = delay
        self.timeout = timeout
        self.metadata_only = metadata_only
        self.batch_size = batch_size

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.verify = False
        # congbobanan.toaan.gov.vn refuses connections from non-VN IPs
        # (ERR_CONNECTION_CLOSED during TLS handshake). Route through a
        # Vietnamese proxy via --proxy or HTTP(S)_PROXY env vars. Running
        # the scraper on a VN-based VPS is the simplest and most reliable
        # option for long 24/7 crawls.
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
        # If no explicit proxy, requests still honors HTTP_PROXY / HTTPS_PROXY
        # from the environment through trust_env (default True).

        self.progress_file = self.output_dir / "progress.json"
        self.csv_file = self.output_dir / "data.csv"
        # At congbobanan's scale (millions of cases) rewriting a single JSON
        # array per checkpoint is impractical, so data.json is written as
        # JSON Lines (one object per line, append-only).
        self.json_file = self.output_dir / "data.json"

        # Backwards-compat: pre-refactor file name was all_metadata.csv.
        legacy_csv = self.output_dir / "all_metadata.csv"
        if legacy_csv.exists() and not self.csv_file.exists():
            legacy_csv.rename(self.csv_file)

    # ----- networking -----

    def _get(self, url: str) -> Optional[requests.Response]:
        try:
            return self.session.get(url, timeout=self.timeout)
        except requests.RequestException as e:
            log.warning("Request failed for %s: %s", url, e)
            return None

    def fetch_metadata(self, case_id: int) -> Optional[CaseMetadata]:
        """Fetch and parse the detail page. Returns None only on HTTP error."""
        url = DETAIL_URL.format(id=case_id)
        resp = self._get(url)
        if resp is None or resp.status_code != 200:
            return None
        return parse_metadata(case_id, resp.text)

    def _find_existing_pdf(self, case_id: int) -> Optional[Path]:
        """Check if a PDF for this case_id already exists (any filename starting with the id)."""
        prefix = f"{case_id}_"
        for p in self.pdf_dir.iterdir():
            if (p.name == f"{case_id}.pdf" or p.name.startswith(prefix)) and p.stat().st_size > 0:
                return p
        return None

    def download_pdf(self, case_id: int, meta: Optional[CaseMetadata] = None) -> Optional[tuple]:
        """Download PDF and return (saved_filename, size_bytes), or None on failure."""
        existing = self._find_existing_pdf(case_id)
        has_good_meta = meta and meta.has_metadata and (meta.ban_an_so or meta.toa_an_xet_xu)

        if existing:
            is_nometa = existing.name.endswith("_nometa.pdf")
            if is_nometa and has_good_meta:
                new_name = build_pdf_name(meta)
                new_path = self.pdf_dir / new_name
                existing.rename(new_path)
                log.info("ID %d: renamed %s -> %s", case_id, existing.name, new_name)
                return new_name, new_path.stat().st_size
            return existing.name, existing.stat().st_size

        url = PDF_URL.format(id=case_id)
        resp = self._get(url)
        if resp is None or resp.status_code != 200:
            return None
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type:
            return None
        if len(resp.content) < 100:
            return None

        if has_good_meta:
            filename = build_pdf_name(meta)
        else:
            filename = f"{case_id}_nometa.pdf"

        pdf_path = self.pdf_dir / filename
        pdf_path.write_bytes(resp.content)
        return filename, len(resp.content)

    def save_metadata(self, meta: CaseMetadata):
        json_path = self.meta_dir / f"{meta.id}.json"
        json_path.write_text(
            json.dumps(asdict(meta), ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ----- progress / resume -----

    def load_progress(self) -> dict:
        """Return {completed: set[int], last_id: int}."""
        if not self.progress_file.exists():
            return {"completed": set(), "last_id": 0}
        data = json.loads(self.progress_file.read_text())
        completed = set(data.get("completed", []))
        last_id = int(data.get("last_id", max(completed) if completed else 0))
        return {"completed": completed, "last_id": last_id}

    def save_progress(self, completed: set[int], last_id: int):
        self.progress_file.write_text(json.dumps({
            "completed": sorted(completed),
            "last_id": last_id,
            "count": len(completed),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        }))

    def is_item_complete(self, case_id: int, progress: dict) -> bool:
        """A case is complete iff its metadata JSON is on disk AND (we're in
        metadata-only mode OR its PDF is on disk). We also require the
        progress file to have recorded it, so a stray leftover file from an
        aborted run doesn't cause us to skip retrying.
        """
        if case_id not in progress["completed"]:
            return False
        if not (self.meta_dir / f"{case_id}.json").exists():
            return False
        if self.metadata_only:
            return True
        return self._find_existing_pdf(case_id) is not None

    # ----- per-item worker -----

    def process_item(self, case_id: int) -> Optional[CaseMetadata]:
        """Download metadata + PDF for one case ID. Returns the metadata on
        success, or None if the case could not be fetched.
        """
        meta = self.fetch_metadata(case_id)
        if meta is None:
            log.debug("ID %d: HTTP error (does not exist)", case_id)
            return None

        if not meta.has_metadata:
            time.sleep(self.delay)
            meta2 = self.fetch_metadata(case_id)
            if meta2 and meta2.has_metadata:
                meta = meta2
            else:
                if self.metadata_only:
                    return None
                time.sleep(self.delay)
                result = self.download_pdf(case_id, meta)
                if result:
                    meta.pdf_saved_as, meta.pdf_size_bytes = result
                    self.save_metadata(meta)
                    return meta
                log.debug("ID %d: no metadata, no PDF", case_id)
                return None

        if not self.metadata_only:
            time.sleep(self.delay)
            result = self.download_pdf(case_id, meta)
            if result:
                meta.pdf_saved_as, meta.pdf_size_bytes = result
            else:
                log.warning("ID %d: metadata OK but PDF download failed", case_id)

        self.save_metadata(meta)
        return meta

    def append_csv(self, meta: CaseMetadata):
        write_header = not self.csv_file.exists()
        with open(self.csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(meta).keys()))
            if write_header:
                writer.writeheader()
            writer.writerow(asdict(meta))

    def append_json(self, meta: CaseMetadata):
        """Append one record to data.json as a JSON Lines entry."""
        with open(self.json_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(meta), ensure_ascii=False) + "\n")

    # ----- main loop -----

    def run(self, start_id: int, end_id: int, resume: bool = True):
        progress = self.load_progress() if resume else {"completed": set(), "last_id": 0}

        # Resume from the case right after the last fully-completed one,
        # but never before the user-specified start.
        resume_from = max(start_id, progress["last_id"] + 1) if progress["last_id"] else start_id
        total = end_id - start_id + 1
        log.info(
            "Starting scrape: IDs %d-%d (%d total, %d already done, resuming from %d)",
            start_id, end_id, total, len(progress["completed"]), resume_from,
        )

        # Build the work list, filtering out anything already fully on disk.
        ids_to_process: list[int] = []
        for cid in range(resume_from, end_id + 1):
            if self.is_item_complete(cid, progress):
                continue
            ids_to_process.append(cid)

        log.info("Remaining: %d cases", len(ids_to_process))
        if not ids_to_process:
            log.info("Nothing to do, all IDs already processed.")
            return

        completed: set[int] = progress["completed"]
        last_id = progress["last_id"]
        success_count = len([i for i in completed if start_id <= i <= end_id])
        failed_count = 0

        def worker_fn(case_id: int):
            time.sleep(self.delay)
            return case_id, self.process_item(case_id)

        for batch_start in range(0, len(ids_to_process), self.batch_size):
            batch = ids_to_process[batch_start : batch_start + self.batch_size]

            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                futures = {executor.submit(worker_fn, cid): cid for cid in batch}
                for future in as_completed(futures):
                    case_id = futures[future]
                    try:
                        cid, meta = future.result()
                        completed.add(cid)
                        last_id = max(last_id, cid)
                        if meta:
                            self.append_csv(meta)
                            self.append_json(meta)
                            success_count += 1
                            log.info(
                                "[%d/%d] ID %d: %s | %s | %s | PDF %d bytes",
                                success_count, total, cid,
                                meta.ban_an_so, meta.loai_vu_viec,
                                meta.toa_an_xet_xu, meta.pdf_size_bytes,
                            )
                        else:
                            failed_count += 1
                            log.debug("[skip] ID %d not found", cid)
                    except Exception as e:
                        failed_count += 1
                        log.error("ID %d raised exception: %s", case_id, e)

            self.save_progress(completed, last_id)
            log.info(
                "Batch checkpoint: %d success, %d failed, %d total completed",
                success_count, failed_count, len(completed),
            )

        log.info(
            "Done. %d succeeded, %d failed/missing out of %d.",
            success_count, failed_count, total,
        )


def main():
    parser = argparse.ArgumentParser(description="congbobanan.toaan.gov.vn scraper")
    parser.add_argument("--start", type=int, default=1, help="Start case ID (default: 1)")
    parser.add_argument("--end", type=int, default=2_100_400, help="End case ID (default: 2100400)")
    parser.add_argument("--output", type=str, default="./data", help="Output directory")
    parser.add_argument("--num-workers", type=int, default=4, help="Concurrent workers (default: 4)")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between requests in seconds")
    parser.add_argument("--timeout", type=int, default=REQUEST_TIMEOUT, help="Request timeout in seconds")
    parser.add_argument("--metadata-only", action="store_true", help="Only download metadata, skip PDFs")
    parser.add_argument("--no-resume", action="store_true", help="Don't resume from previous progress")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for progress checkpoints")
    parser.add_argument("--test", type=int, help="Test with a single case ID")
    parser.add_argument(
        "--proxy", type=str, default=None,
        help="HTTP/HTTPS/SOCKS proxy URL (e.g. http://user:pass@host:port, "
             "socks5h://host:port). Required when running from outside Vietnam; "
             "HTTP_PROXY/HTTPS_PROXY env vars are also honored.",
    )

    args = parser.parse_args()

    scraper = CongboScraper(
        output_dir=args.output,
        num_workers=args.num_workers,
        delay=args.delay,
        timeout=args.timeout,
        metadata_only=args.metadata_only,
        batch_size=args.batch_size,
        proxy=args.proxy,
    )

    if args.test:
        meta = scraper.process_item(args.test)
        if meta:
            print(json.dumps(asdict(meta), ensure_ascii=False, indent=2))
        else:
            print(f"Case ID {args.test} not found or failed.")
        return

    scraper.run(
        start_id=args.start,
        end_id=args.end,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    main()
