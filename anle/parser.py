#!/usr/bin/env python3
"""
Parse anle PDFs with NVIDIA Nemotron Parse (nvidia/nemotron-parse) and write
one structured JSON file per case.

Based on the cookbook:
  https://github.com/NVIDIA-NeMo/Nemotron/blob/main/usage-cookbook/
    Nemotron-Parse-v1.1/build_general_usage_cookbook.ipynb

Inputs  (defaults):
  ./data/pdfs/<doc_name>.pdf            (produced by anle/scraper.py)
  ./data/data.csv                       (per-case metadata, optional)

Outputs:
  ./data/json/<doc_name>.json           (one structured record per case)

Each output JSON contains:
  {
    "doc_name":    "TAND349038",
    "source_pdf":  "pdfs/TAND349038.pdf",
    "model":       "nvidia/nemotron-parse",
    "parsed_at":   "2026-04-23T12:34:56Z",
    "metadata":    {...scraper metadata merged from data.csv...},
    "num_pages":   12,
    "pages": [
      {
        "page_number": 1,
        "blocks": [
          {"type": "Title", "text": "...", "bbox": {xmin, ymin, xmax, ymax}},
          ...
        ],
        "markdown": "..."
      }, ...
    ],
    "markdown":    "...all pages joined..."
  }

Usage:
    export NVIDIA_API_KEY=nvapi-...
    python anle/parser.py                       # parse every PDF in data/pdfs
    python anle/parser.py --num-workers 4       # 4 concurrent API calls
    python anle/parser.py --limit 10            # only parse 10 PDFs (smoke test)
    python anle/parser.py --doc TAND349038      # parse one specific case
"""

import argparse
import base64
import csv
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import requests

try:
    import fitz  # PyMuPDF
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "PyMuPDF is required: pip install -r anle/requirements.txt"
    ) from exc

try:
    from PIL import Image
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Pillow is required: pip install -r anle/requirements.txt"
    ) from exc


DEFAULT_MODEL = "nvidia/nemotron-parse"
DEFAULT_NVAI_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_MAX_TOKENS = 3500
DEFAULT_DPI = 300
# Matches the cookbook's canvas so bbox normalization stays consistent.
TARGET_CANVAS = (1536, 2048)
API_TIMEOUT = 180

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ----- PDF rendering & image encoding -----

def pdf_page_to_image(
    pdf_path: Path,
    page_index: int,
    dpi: int = DEFAULT_DPI,
    target_size: tuple[int, int] = TARGET_CANVAS,
) -> Image.Image:
    """Render a PDF page to an RGB image, centered on a fixed-size white canvas.

    The canvas size matches the cookbook so the model sees images at the
    dimensions it was tuned for.
    """
    doc = fitz.open(pdf_path)
    try:
        page = doc.load_page(page_index)
        zoom = dpi / 72.0
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
        src = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    finally:
        doc.close()

    canvas = Image.new("RGB", target_size, (255, 255, 255))
    src.thumbnail(target_size, Image.Resampling.LANCZOS)
    x = (target_size[0] - src.width) // 2
    y = (target_size[1] - src.height) // 2
    canvas.paste(src, (x, y))
    return canvas


def encode_image_to_base64(image: Image.Image, fmt: str = "PNG") -> str:
    buf = BytesIO()
    image.save(buf, format=fmt)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def pdf_num_pages(pdf_path: Path) -> int:
    doc = fitz.open(pdf_path)
    try:
        return doc.page_count
    finally:
        doc.close()


# ----- Nemotron Parse API -----

class NemotronParseClient:
    """Thin client around the chat-completions endpoint used by Nemotron Parse."""

    def __init__(
        self,
        api_key: str,
        api_url: str = DEFAULT_NVAI_URL,
        model: str = DEFAULT_MODEL,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        timeout: int = API_TIMEOUT,
        retries: int = 3,
        backoff: float = 2.0,
    ):
        self.api_url = api_url.rstrip("/")
        self.model = model
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })

    def parse_image(self, base64_image: str) -> list[dict[str, Any]]:
        """Call the model on a single base64-encoded PNG and return block list."""
        messages = [{
            "role": "user",
            "content": [{
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{base64_image}"},
            }],
        }]
        tools = [{"type": "function", "function": {"name": "markdown_bbox"}}]
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": self.max_tokens,
            "temperature": 0,
            "tools": tools,
        }

        last_err: Optional[Exception] = None
        for attempt in range(self.retries):
            try:
                resp = self.session.post(
                    f"{self.api_url}/chat/completions",
                    json=payload,
                    timeout=self.timeout,
                )
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    raise requests.HTTPError(
                        f"{resp.status_code}: {resp.text[:200]}", response=resp
                    )
                resp.raise_for_status()
                response_json = resp.json()
                return _extract_blocks(response_json)
            except requests.RequestException as e:
                last_err = e
                if attempt < self.retries - 1:
                    sleep = self.backoff * (2 ** attempt)
                    log.warning(
                        "Nemotron API attempt %d failed (%s); retrying in %.1fs",
                        attempt + 1, e, sleep,
                    )
                    time.sleep(sleep)
        log.error("Nemotron API giving up after %d retries: %s", self.retries, last_err)
        return []


def _extract_blocks(response_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Pull the parsed block list out of the tool-calls response."""
    choices = response_json.get("choices") or []
    if not choices:
        return []
    message = choices[0].get("message") or {}
    tool_calls = message.get("tool_calls") or []
    if not tool_calls:
        return []
    arguments_str = (
        (tool_calls[0] or {}).get("function", {}).get("arguments", "[]")
    )
    try:
        parsed = json.loads(arguments_str)
    except json.JSONDecodeError:
        log.warning("Could not decode tool_call arguments: %r", arguments_str[:200])
        return []
    # The model sometimes returns a singleton-wrapped list.
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], list):
        return parsed[0]
    return parsed if isinstance(parsed, list) else []


# ----- Markdown assembly (trimmed-down port from the cookbook) -----

def _clean_md(text: str) -> str:
    text = re.sub(r"\\([#>*_`~\-+!\[\]()])", r"\1", text)
    text = text.replace("\u2022", "-")
    return text.strip()


def blocks_to_markdown(blocks: list[dict[str, Any]]) -> str:
    """Assemble one page's blocks into a Markdown string.

    Tables are left as their raw LaTeX tabular (wrapped in <pre><code>) so the
    output JSON stays machine-friendly; downstream tools can convert to HTML
    or DataFrame as needed.
    """
    lines: list[str] = []
    in_list = False

    def close_list():
        nonlocal in_list
        if in_list:
            lines.append("")
            in_list = False

    for b in blocks:
        cat = b.get("type", "Text")
        text = (b.get("text") or "").strip()
        if not text:
            continue

        if cat == "Table":
            close_list()
            lines.append(f"<pre><code class=\"latex\">{text}</code></pre>")
            lines.append("")
            continue
        if cat == "Formula":
            close_list()
            lines.append(f"<pre><code>$$ {text} $$</code></pre>")
            lines.append("")
            continue

        t = _clean_md(text)
        if cat == "Title":
            close_list()
            lines.append(t if t.lstrip().startswith("#") else f"# {t}")
            lines.append("")
        elif cat == "Section-header":
            close_list()
            lines.append(t if t.lstrip().startswith("##") else f"## {t}")
            lines.append("")
        elif cat == "List-item":
            lines.append(f"- {t}")
            in_list = True
        elif cat == "Caption":
            close_list()
            lines.append(f"> Caption: {t}")
            lines.append("")
        elif cat == "Footnote":
            close_list()
            lines.append(f"<p><small>[Footnote] {t}</small></p>")
            lines.append("")
        else:
            close_list()
            lines.append(t)
            lines.append("")

    if in_list:
        lines.append("")
    return "\n".join(lines).strip()


# ----- Orchestrator -----

@dataclass
class ParseTask:
    doc_name: str
    pdf_path: Path
    metadata: dict[str, Any]


class AnleParser:
    """Parse every PDF in data/pdfs/ -> one JSON file per case in data/json/."""

    def __init__(
        self,
        output_dir: str,
        api_key: str,
        api_url: str = DEFAULT_NVAI_URL,
        model: str = DEFAULT_MODEL,
        num_workers: int = 2,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        dpi: int = DEFAULT_DPI,
    ):
        self.output_dir = Path(output_dir)
        self.pdf_dir = self.output_dir / "pdfs"
        self.json_dir = self.output_dir / "json"
        self.num_workers = num_workers
        self.dpi = dpi

        if not self.pdf_dir.exists():
            raise FileNotFoundError(
                f"PDF directory not found: {self.pdf_dir}. "
                "Run `python anle/scraper.py` first."
            )
        self.json_dir.mkdir(parents=True, exist_ok=True)

        self.client = NemotronParseClient(
            api_key=api_key,
            api_url=api_url,
            model=model,
            max_tokens=max_tokens,
        )
        self.metadata_index = self._load_metadata_index()

    # ----- metadata lookup -----

    def _load_metadata_index(self) -> dict[str, dict[str, Any]]:
        """Build a {doc_name: row_dict} map from data.csv (if present)."""
        csv_path = self.output_dir / "data.csv"
        if not csv_path.exists():
            log.info("No data.csv found next to PDFs; output JSON will omit scraper metadata.")
            return {}
        index: dict[str, dict[str, Any]] = {}
        with open(csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                doc = row.get("doc_name", "").strip()
                if doc:
                    index[doc] = row
        log.info("Loaded metadata for %d cases from %s", len(index), csv_path)
        return index

    # ----- enumeration -----

    def list_tasks(self, only_doc: Optional[str] = None) -> list[ParseTask]:
        tasks: list[ParseTask] = []
        for pdf_path in sorted(self.pdf_dir.glob("*.pdf")):
            if pdf_path.stat().st_size <= 0:
                continue
            doc_name = pdf_path.stem
            if only_doc and doc_name != only_doc:
                continue
            tasks.append(ParseTask(
                doc_name=doc_name,
                pdf_path=pdf_path,
                metadata=self.metadata_index.get(doc_name, {}),
            ))
        return tasks

    # ----- resume / skip -----

    def is_item_complete(self, doc_name: str) -> bool:
        """A case is complete if its output JSON exists, is valid, and records
        at least one parsed page."""
        path = self.json_dir / f"{doc_name}.json"
        if not path.exists() or path.stat().st_size == 0:
            return False
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return False
        return int(data.get("num_pages", 0)) > 0 and bool(data.get("pages"))

    # ----- per-item worker -----

    def process_item(self, task: ParseTask) -> bool:
        """Parse all pages of one PDF and atomically write the JSON."""
        try:
            num_pages = pdf_num_pages(task.pdf_path)
        except Exception as e:
            log.error("%s: failed to open PDF (%s)", task.doc_name, e)
            return False

        pages: list[dict[str, Any]] = []
        for page_idx in range(num_pages):
            try:
                img = pdf_page_to_image(task.pdf_path, page_idx, dpi=self.dpi)
                b64 = encode_image_to_base64(img)
                blocks = self.client.parse_image(b64)
            except Exception as e:
                log.error(
                    "%s page %d: render/parse failed (%s)",
                    task.doc_name, page_idx + 1, e,
                )
                blocks = []
            pages.append({
                "page_number": page_idx + 1,
                "blocks": blocks,
                "markdown": blocks_to_markdown(blocks),
            })

        record = {
            "doc_name": task.doc_name,
            "source_pdf": str(task.pdf_path.relative_to(self.output_dir)),
            "model": self.client.model,
            "parsed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "metadata": task.metadata,
            "num_pages": num_pages,
            "pages": pages,
            "markdown": "\n\n".join(p["markdown"] for p in pages if p["markdown"]),
        }

        out_path = self.json_dir / f"{task.doc_name}.json"
        tmp_path = out_path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp_path.rename(out_path)
        total_blocks = sum(len(p["blocks"]) for p in pages)
        log.info(
            "%s: %d pages, %d blocks -> %s",
            task.doc_name, num_pages, total_blocks, out_path.name,
        )
        return True

    # ----- main loop -----

    def run(
        self,
        only_doc: Optional[str] = None,
        limit: Optional[int] = None,
        resume: bool = True,
    ):
        all_tasks = self.list_tasks(only_doc=only_doc)
        if only_doc and not all_tasks:
            log.error("No PDF matched --doc %s under %s", only_doc, self.pdf_dir)
            return

        pending: list[ParseTask] = []
        already_done = 0
        for t in all_tasks:
            if resume and self.is_item_complete(t.doc_name):
                already_done += 1
                continue
            pending.append(t)

        if limit is not None:
            pending = pending[:limit]

        log.info(
            "Found %d PDFs, %d already parsed, %d to process with %d workers",
            len(all_tasks), already_done, len(pending), self.num_workers,
        )
        if not pending:
            log.info("Nothing to do.")
            return

        success = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=self.num_workers) as ex:
            futures = {ex.submit(self.process_item, t): t for t in pending}
            for fut in as_completed(futures):
                task = futures[fut]
                try:
                    if fut.result():
                        success += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    log.error("%s raised exception: %s", task.doc_name, e)

        log.info("Done. %d succeeded, %d failed (of %d).", success, failed, len(pending))


def main():
    parser = argparse.ArgumentParser(
        description="Parse anle PDFs with NVIDIA Nemotron Parse; "
                    "write one JSON per case to data/json/.",
    )
    parser.add_argument(
        "--output", type=str, default="./data",
        help="Data directory (looks for pdfs/ and writes to json/). Default: ./data",
    )
    parser.add_argument(
        "--num-workers", type=int, default=2,
        help="Concurrent API calls (default: 2). Keep modest to respect rate limits.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process at most N PDFs (useful for smoke tests).",
    )
    parser.add_argument(
        "--doc", type=str, default=None,
        help="Only parse this doc_name (e.g. TAND349038).",
    )
    parser.add_argument(
        "--no-resume", action="store_true",
        help="Re-parse PDFs even if their JSON already exists.",
    )
    parser.add_argument(
        "--api-url", type=str, default=os.environ.get("NVAI_URL", DEFAULT_NVAI_URL),
        help=f"NVIDIA API base URL (default: env NVAI_URL or {DEFAULT_NVAI_URL}).",
    )
    parser.add_argument(
        "--model", type=str, default=DEFAULT_MODEL,
        help=f"Model name (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
        help=f"max_tokens per page request (default: {DEFAULT_MAX_TOKENS}).",
    )
    parser.add_argument(
        "--dpi", type=int, default=DEFAULT_DPI,
        help=f"PDF render DPI (default: {DEFAULT_DPI}).",
    )
    args = parser.parse_args()

    api_key = os.environ.get("NVIDIA_API_KEY", "").strip()
    if not api_key:
        parser.error(
            "NVIDIA_API_KEY env var is not set. Get a key at "
            "https://build.nvidia.com/ and `export NVIDIA_API_KEY=nvapi-...`."
        )

    runner = AnleParser(
        output_dir=args.output,
        api_key=api_key,
        api_url=args.api_url,
        model=args.model,
        num_workers=args.num_workers,
        max_tokens=args.max_tokens,
        dpi=args.dpi,
    )
    runner.run(
        only_doc=args.doc,
        limit=args.limit,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    main()
