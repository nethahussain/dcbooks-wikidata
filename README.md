# DC Books → Wikidata

A Python scraper that extracts book metadata from [DC Books](https://dcbooks.com/) (Kerala's leading Malayalam publisher) and outputs a Wikidata-ready Excel file for batch upload via [QuickStatements](https://quickstatements.toolforge.org/).

## What it does

1. Fetches all ~3,800 book URLs from `dcbookstore.com/sitemap.xml`
2. Solves the Sucuri CloudProxy WAF challenge using Node.js
3. Scrapes each book page for full metadata
4. Outputs a formatted `.xlsx` file with Wikidata property mappings

## Fields extracted

| Field | Wikidata property | Coverage |
|-------|-------------------|----------|
| Instance of (literary work) | P31 | 100% |
| Title (transliterated) | Label (en) | 100% |
| Author | P50 | ~100% |
| ISBN-13 | P212 | ~97% |
| ISBN-10 | P957 | ~25% |
| Publisher / imprint | P123 | 100% |
| Language | P407 | 100% |
| Publication date | P577 | ~100% |
| Number of pages | P1104 | ~100% |
| Genre / category | P136 | ~98% |
| Edition | P393 | ~100% |
| Binding format | P437 | ~100% |
| Malayalam description | Description (ml) | one-sentence, 100% |
| Cover image URL | — | ~100% |
| Source URL | S854 | 100% |

## Requirements

- Python 3.8+
- Node.js (for Sucuri WAF bypass)

## Setup

```bash
git clone https://github.com/nethahussain/dcbooks-wikidata.git
cd dcbooks-wikidata
python3 -m venv venv
source venv/bin/activate
pip install requests beautifulsoup4 openpyxl lxml cloudscraper
```

## Usage

```bash
# Test with a small batch
python3 dcbooks_scraper.py --limit 100

# Scrape all ~3,800 books (takes several hours)
python3 dcbooks_scraper.py

# Resume after interruption (progress auto-saves every 50 books)
python3 dcbooks_scraper.py --resume

# Custom output filename
python3 dcbooks_scraper.py --output my_output.xlsx

# Run in background
nohup python3 dcbooks_scraper.py > scraper.log 2>&1 &
tail -f scraper.log
```

## Output

The Excel file has three sheets:

- **Wikidata Upload** — columns mapped to Wikidata properties (P31, P50, P212, P123, P407, etc.), with QID values for languages, genres, and country of origin. Ready for QuickStatements.
- **Raw Data** — unprocessed scraped fields for reference and debugging.
- **Wikidata Property Legend** — explains each column, property ID, expected format, and example values.

The full dataset of 3,641 books is included in [`data/dcbooks_wikidata.xlsx`](data/dcbooks_wikidata.xlsx).

A flattened, **upload-ready** [`data/dcbooks_quickstatements.csv`](data/dcbooks_quickstatements.csv) is also provided — paste it directly into [QuickStatements](https://quickstatements.toolforge.org/) (CSV mode). Each row creates a new item (blank `qid`) with P31, labels/descriptions, ISBNs, language, country, publication date, pages, edition, and the valid genre QIDs; the source URL is attached as a reference on P31. Columns that still hold plain text (P50 author, P123 publisher, P437 format, S248) are intentionally omitted — see blockers below. **Test a few rows and check for existing items before bulk-creating.**

## Data cleaning applied

The published dataset has been cleaned beyond the raw scraper output:

- **Instance of (P31)** set to `Q7725634` (*literary work*) for all records — not the generic *written work*.
- **Malayalam descriptions (Description ml)** rewritten as a single clean sentence each, with proper head nouns (e.g. *malayala pachaka pusthakam* rather than a bare topic-genre). No truncated blurbs.
- **Genre (P136)** QIDs verified against the Wikidata API and corrected — several scraped QIDs pointed to unrelated items (a town, a musician, a sports event, etc.) and were replaced with the correct genre items; one invalid QID was removed.
- **Publication dates (P577)** normalized to day precision (`+YYYY-MM-DDT00:00:00Z/11`), with no spurious time component.

### Known remaining blockers for direct QuickStatements import

These columns still hold plain text where QuickStatements expects QIDs, and need resolving before upload:

- **P50 (author)** and **P123 (publisher)** — plain names, not QIDs (run `link_authors_wikidata.py` for authors).
- **P136 (genre)** — ~533 cells still contain non-QID category tokens.
- **P437 (binding format)** and **S248 (stated in)** — plain labels, not QIDs.
- A handful of malformed ISBN-13s and one malformed date remain.

## How it works

DC Books operates two websites:

- **dcbooks.com** — WordPress catalogue with ~238 featured books (limited, no pagination)
- **dcbookstore.com** — full online store with ~3,800+ books, protected by [Sucuri](https://sucuri.net/) WAF

The scraper targets dcbookstore.com via its public `sitemap.xml`. The Sucuri challenge is a Base64-encoded JavaScript snippet that computes a cookie value — this is decoded and evaluated using Node.js to obtain the session cookie, after which all pages are accessible via standard HTTP requests.

## Notes on Malayalam titles

dcbookstore.com uses English transliterations (e.g. "Aadujeevitham") rather than Malayalam script (ആടുജീവിതം). The current dataset includes transliterated Malayalam titles for all 3,641 books (generated via Google Input Tools) in the `Label (ml)` column. For actual Malayalam script titles as used by readers and libraries, use the `scrape_keralabookstore.py` script which fetches them from keralabookstore.com and merges them by ISBN.

## Getting Malayalam titles from Kerala Book Store

keralabookstore.com has actual Malayalam script titles (ദേവദാസ്, ആടുജീവിതം) and Malayalam author names — something dcbookstore.com lacks. A separate scraper fetches these and merges them into the Excel by ISBN:

```bash
source venv/bin/activate

# Scrape DC Books titles from keralabookstore.com
python3 scrape_keralabookstore.py

# Scrape and merge directly into the Wikidata Excel
python3 scrape_keralabookstore.py --merge data/dcbooks_wikidata.xlsx
```

This must be run locally — keralabookstore.com blocks cloud/datacenter IPs.

## Linking authors to Wikidata

A separate script looks up each author against Wikidata and adds their QID to a new column:

```bash
source venv/bin/activate
python3 link_authors_wikidata.py
```

This searches all 1,761 unique authors against Wikidata's API, caches results in `author_qid_cache.json`, and adds a `P50_QID (author Wikidata ID)` column to the Excel file. Takes ~10 minutes on first run; subsequent runs use the cache.

## QuickStatements upload files

The cleaned dataset is split by language for safe upload (QuickStatements **V1** format, tab-separated):

| File | Books | Status |
|------|------:|--------|
| `data/dcbooks_quickstatements_MALAYALAM.txt` | 3,310 | Ready for bulk upload — zero title matches against existing Wikidata items |
| `data/dcbooks_quickstatements_ENGLISH.txt` | 311 | Held for title review — likely already exist on Wikidata as translations/reprints |

Each book uses `P2093` (author name string) so authors upload without needing QIDs, includes only checksum-valid ISBNs (`P212`/`P957`), and carries multi-value `P136` genres and `P437` binding format where known. The 20 books already on Wikidata (matched by ISBN) were removed during dedup.

## Wikidata integration

Before uploading:

1. Run `link_authors_wikidata.py` to auto-link authors to their Wikidata QIDs
2. Check if books already exist on Wikidata (search by ISBN-13 using SPARQL)
3. Replace publisher strings with QIDs (e.g. DC Books = [Q5203520](https://www.wikidata.org/wiki/Q5203520))
4. Review auto-generated descriptions
5. Upload via [QuickStatements V2](https://quickstatements.toolforge.org/)

## License

[CC0 1.0 Universal](LICENSE) — public domain dedication.

## See also

- [DC Books on Wikidata](https://www.wikidata.org/wiki/Q5203520)
- [DC Books website](https://dcbooks.com/)
- [DC Bookstore](https://dcbookstore.com/)
