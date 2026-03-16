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
| Malayalam description | Description (ml) | ~80% |
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

## How it works

DC Books operates two websites:

- **dcbooks.com** — WordPress catalogue with ~238 featured books (limited, no pagination)
- **dcbookstore.com** — full online store with ~3,800+ books, protected by [Sucuri](https://sucuri.net/) WAF

The scraper targets dcbookstore.com via its public `sitemap.xml`. The Sucuri challenge is a Base64-encoded JavaScript snippet that computes a cookie value — this is decoded and evaluated using Node.js to obtain the session cookie, after which all pages are accessible via standard HTTP requests.

## Notes on Malayalam titles

Neither site stores explicit Malayalam-script titles. Both use English transliterations (e.g. "AADUJEEVITHAM" rather than "ആടുജീവിതം"). The Malayalam text that _is_ available comes from book summaries, captured in the `Description (ml)` column. Actual Malayalam script titles would need to be added manually or via a separate lookup.

## Wikidata integration

Before uploading:

1. Check if books already exist on Wikidata (search by ISBN-13 using SPARQL)
2. Replace author name strings with Wikidata QIDs where the author item exists
3. Replace publisher strings with QIDs (e.g. DC Books = [Q3075043](https://www.wikidata.org/wiki/Q3075043))
4. Review auto-generated descriptions
5. Upload via [QuickStatements V2](https://quickstatements.toolforge.org/)

## License

[CC0 1.0 Universal](LICENSE) — public domain dedication.

## See also

- [DC Books on Wikidata](https://www.wikidata.org/wiki/Q3075043)
- [DC Books website](https://dcbooks.com/)
- [DC Bookstore](https://dcbookstore.com/)
