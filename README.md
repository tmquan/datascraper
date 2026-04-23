# datascrapper

Vietnamese court / judicial data scrapers.

## Layout

```
datascrapper/
├── anle/              # anle.toaan.gov.vn  (precedent source documents)
│   ├── scraper.py
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

## Adding a new scraper

Copy either `anle/scraper.py` or `congbobanan/scraper.py` as a template
and adapt `fetch_*`, `process_item`, and `is_item_complete` to the new
source. Keep the `--num-workers` flag, the `progress.json` format, the
`data/` output layout, and the class method names so operators get a
consistent experience.
