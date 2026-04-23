# datascrapper

Vietnamese court / judicial data scrapers.

## Layout

```
datascrapper/
├── anle/              # anle.toaan.gov.vn  (precedent source documents)
│   ├── scraper.py
│   ├── parser.py          # Nemotron Parse -> data/json/<doc>.json
│   └── requirements.txt
└── congbobanan/       # congbobanan.toaan.gov.vn  (published judgments)
    ├── scraper.py
    └── requirements.txt
```

Each scraper is a self-contained `scraper.py`. They share the same
architectural pattern so behavior and CLI flags stay consistent:

- a `*Scraper` class with `load_progress` / `save_progress` /
  `is_item_complete` / `process_item` / `run`
- a `--num-workers` flag that controls the `ThreadPoolExecutor` pool size
- resume is on by default and keyed off `progress.json` inside the output
  directory; pass `--no-resume` to start fresh
- `is_item_complete` inspects the filesystem (metadata JSON, PDF on disk)
  so partially-downloaded items are retried while fully-downloaded items
  are skipped

## Output layout (shared)

Both scrapers default to `--output ./data` and produce the same set of
files so downstream tooling can treat them uniformly:

```
data/
├── pdfs/             # downloaded PDFs
├── data.csv          # aggregate CSV of all records (canonical)
├── data.json         # aggregate JSON of all records
├── progress.json     # resume checkpoint ({completed, last_id, ...})
└── metadata/         # congbobanan-only: per-case detail JSONs
```

Notes on `data.json`:

- `anle/` writes it as a standard JSON **array** (the dataset is small,
  a few thousand records).
- `congbobanan/` writes it as **JSON Lines** (one JSON object per line,
  append-only) because at 2 M+ records rewriting a single array on every
  checkpoint is impractical. Read it with e.g. `pandas.read_json(..., lines=True)`.

Each scraper defines its own notion of an "item":

| scraper       | item         | resume granularity |
| ------------- | ------------ | ------------------ |
| `anle`        | listing page | page number        |
| `congbobanan` | case ID      | case ID            |

## Usage

```bash
# one-time install
pip install -r anle/requirements.txt
pip install -r congbobanan/requirements.txt

# anle: scrape all pages, 4 PDF workers, into ./data
python anle/scraper.py --num-workers 4

# congbobanan: scrape a slice of IDs, metadata only
python congbobanan/scraper.py --start 1000000 --end 1000100 --metadata-only

# force a restart (ignore previous progress.json)
python anle/scraper.py --no-resume
```

Both scrapers target Vietnamese government sites that occasionally block
aggressive traffic; keep `--num-workers` modest (2-4) and `--delay` > 0.

## Geographic access (Vietnam-only hosts)

`congbobanan.toaan.gov.vn` refuses TLS connections from non-VN IPs
(`ERR_CONNECTION_CLOSED` during the handshake). `anle.toaan.gov.vn` is
usually globally reachable but can behave similarly on some networks.

You have three practical options, in order of preference for long 24/7
crawls:

1. **Run on a VN-based VPS** (recommended). Providers with Vietnam
   regions include Vultr (Hanoi), BizFly Cloud, Viettel IDC, VNG Cloud,
   FPT Cloud. A $5-10/month VPS is cheaper and faster than any proxy and
   needs zero code changes.

2. **HTTP/HTTPS proxy with a VN exit.** Pass it via `--proxy` or set
   `HTTP_PROXY` / `HTTPS_PROXY` env vars:

   ```bash
   python congbobanan/scraper.py --proxy http://user:pass@vn-proxy.example:8080
   # or
   HTTPS_PROXY=http://user:pass@vn-proxy.example:8080 \
     python congbobanan/scraper.py
   ```

   Residential proxy services that sell VN exits: Bright Data, Oxylabs,
   Smartproxy, IPRoyal. Pick sticky sessions so congbobanan doesn't see
   IP churn mid-download.

3. **SOCKS5 proxy / VPN.** The `requests[socks]` extra is already in
   `requirements.txt`, so `socks5h://` URLs work out of the box (the
   `h` variant resolves DNS through the proxy, which matters for
   geo-locked hostnames):

   ```bash
   python congbobanan/scraper.py --proxy socks5h://127.0.0.1:1080
   ```

   For a VPN, run the scraper on a machine that has the VN VPN active
   and leave `--proxy` unset.

Sanity check before starting a long run:

```bash
curl --proxy http://user:pass@vn-proxy.example:8080 \
     -I https://congbobanan.toaan.gov.vn/
# expect HTTP/1.1 200 OK (or 302/301) — not ERR_CONNECTION_CLOSED
```

## Focusing on specific case categories (congbobanan)

The congbobanan corpus is ~2 M cases across every legal category. To avoid
wasting bandwidth and disk on cases you don't care about, the scraper
accepts `--categories` (and/or `--keywords`) and will only download the
full PDF for matching cases:

```bash
# fraud + murder only, 4 workers, resuming from progress.json
python congbobanan/scraper.py --num-workers 4 --categories fraud,murder

# add extra keywords on top of a preset
python congbobanan/scraper.py --categories murder \
  --keywords "giết người cướp tài sản,giết nhiều người"
```

Behavior when a filter is set:

- The metadata detail page is still fetched for every ID (that's how we
  classify), and `metadata/<id>.json` is still written.
- Only matching cases get the PDF downloaded and appended to `data.csv`
  and `data.json`. A `matched_categories` column records which preset
  hit (e.g. `["fraud"]`).
- Non-matching cases are still recorded in `progress.json` so subsequent
  runs won't re-fetch them.

Presets:

| preset   | keywords                                                                 |
| -------- | ------------------------------------------------------------------------ |
| `fraud`  | Lừa đảo chiếm đoạt tài sản (174), Lừa dối khách hàng (198), Lạm dụng tín nhiệm (175) |
| `murder` | Giết người (123), infanticide (124), crime-of-passion (125), excessive self-defense (126) |

Matching is case-insensitive Unicode-NFC substring match against
`ten_ban_an`, `quan_he_phap_luat`, `loai_vu_viec`, `thong_tin_vu_viec`,
and `ban_an_so`. Extend `CATEGORY_KEYWORDS` in `congbobanan/scraper.py`
to add more presets.

### Re-filtering an existing crawl

If you've already scraped a large ID range without a filter and now want
to extract the fraud + murder subset, use `--rebuild-filtered`. It runs
entirely offline against `data/metadata/*.json`:

```bash
python congbobanan/scraper.py --output ./data \
  --categories fraud,murder --rebuild-filtered
```

That rewrites `data/data.csv` and `data/data.json` with only the matching
cases (and back-fills `matched_categories` into the per-case JSONs). No
network access required, so this also works behind geo-blocks.

## Parsing anle PDFs with Nemotron Parse

Once `anle/scraper.py` has populated `anle/data/pdfs/`, run
`anle/parser.py` to extract structured content from each PDF via NVIDIA
Nemotron Parse (`nvidia/nemotron-parse`). One JSON file per case is
written to `anle/data/json/`:

```bash
export NVIDIA_API_KEY=nvapi-...
python anle/parser.py --num-workers 4         # parse everything in data/pdfs
python anle/parser.py --limit 5               # smoke test on 5 PDFs
python anle/parser.py --doc TAND349038        # parse one specific case
python anle/parser.py --no-resume             # re-parse even if JSON exists
```

The output schema is one file per case:

```
anle/data/json/<doc_name>.json
  {
    "doc_name":   "...",
    "source_pdf": "pdfs/....pdf",
    "model":      "nvidia/nemotron-parse",
    "parsed_at":  "2026-...Z",
    "metadata":   {...scraper metadata merged from data.csv...},
    "num_pages":  N,
    "pages": [
      {
        "page_number": 1,
        "blocks": [ {"type":"Title","text":"...","bbox":{...}}, ... ],
        "markdown": "..."
      }, ...
    ],
    "markdown": "...all pages joined..."
  }
```

Resume is on by default: `is_item_complete(doc_name)` skips a PDF if its
JSON already exists, parses as valid, and has `num_pages > 0`. Pass
`--no-resume` to force re-parsing.

Based on the cookbook:
<https://github.com/NVIDIA-NeMo/Nemotron/blob/main/usage-cookbook/Nemotron-Parse-v1.1/build_general_usage_cookbook.ipynb>.

## Adding a new scraper

Copy either `anle/scraper.py` or `congbobanan/scraper.py` as a template
and adapt `fetch_*`, `process_item`, and `is_item_complete` to the new
source. Keep the `--num-workers` flag, the `progress.json` format, the
`data/` output layout, and the class method names so operators get a
consistent experience.
