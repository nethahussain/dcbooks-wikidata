# DC Books / Malayalam Books → Wikidata — Project Status

_Handoff doc. Paste-friendly context for resuming in a fresh Claude session._

## Where things are
- **Repo:** https://github.com/nethahussain/dcbooks-wikidata (all work committed here)
- **Working folder:** `~/Desktop/GitHub/dcbooks-wikidata` (OneDrive-synced → also on Mac Studio)
- Upload tool: QuickStatements **V1** mode, **pipe (`|`) separated** (tabs get mangled on browser copy → "No valid commands found").

## DC Books — DONE ✅
**2,800 book-edition items live on Wikidata, fully cleaned.** All migrated from the original (wrong) single literary-work model to the correct **book-edition** model:
- `P31 = Q57933693` (book edition), not Q7725634 (literary work)
- ISBN `P212`/`P957` **hyphenated** (format constraint); authors via `P2093` (name string)
- Removed work-level props that conflict on an edition: country `P495`, genre `P136`, form-of-creative-work `P7937`
- Each item keeps a `dcbookstore.com` reference URL (`S854`/P854) — the fingerprint of this set

### Health-check query (run at query.wikidata.org — expect all "still…" = 0)
```sparql
SELECT * WHERE {
  { SELECT (COUNT(DISTINCT ?i) AS ?total) WHERE { ?i wdt:P407 wd:Q36236 ; p:P31/prov:wasDerivedFrom/pr:P854 ?u . FILTER(STRSTARTS(STR(?u),"https://dcbookstore.com")) } }
  { SELECT (COUNT(DISTINCT ?i) AS ?bookEditions) WHERE { ?i wdt:P407 wd:Q36236 ; p:P31/prov:wasDerivedFrom/pr:P854 ?u ; wdt:P31 wd:Q57933693 . FILTER(STRSTARTS(STR(?u),"https://dcbookstore.com")) } }
  { SELECT (COUNT(DISTINCT ?i) AS ?stillLiteraryWork) WHERE { ?i wdt:P407 wd:Q36236 ; p:P31/prov:wasDerivedFrom/pr:P854 ?u ; wdt:P31 wd:Q7725634 . FILTER(STRSTARTS(STR(?u),"https://dcbookstore.com")) } }
  { SELECT (COUNT(DISTINCT ?i) AS ?stillCountry) WHERE { ?i wdt:P407 wd:Q36236 ; p:P31/prov:wasDerivedFrom/pr:P854 ?u ; wdt:P495 ?c . FILTER(STRSTARTS(STR(?u),"https://dcbookstore.com")) } }
  { SELECT (COUNT(DISTINCT ?i) AS ?stillGenre) WHERE { ?i wdt:P407 wd:Q36236 ; p:P31/prov:wasDerivedFrom/pr:P854 ?u ; wdt:P136 ?g . FILTER(STRSTARTS(STR(?u),"https://dcbookstore.com")) } }
  { SELECT (COUNT(DISTINCT ?i) AS ?stillFormOfWork) WHERE { ?i wdt:P407 wd:Q36236 ; p:P31/prov:wasDerivedFrom/pr:P854 ?u ; wdt:P7937 ?f . FILTER(STRSTARTS(STR(?u),"https://dcbookstore.com")) } }
  { SELECT (COUNT(DISTINCT ?i) AS ?bareISBN) WHERE { ?i wdt:P407 wd:Q36236 ; p:P31/prov:wasDerivedFrom/pr:P854 ?u . FILTER(STRSTARTS(STR(?u),"https://dcbookstore.com")) { ?i wdt:P212 ?a FILTER(!CONTAINS(STR(?a),"-")) } UNION { ?i wdt:P957 ?b FILTER(!CONTAINS(STR(?b),"-")) } } }
}
```
> **WDQS lags** live edits by minutes–hours. If a count looks wrong but a spot-checked item page is clean, it's lag — wait and re-run, don't re-edit.

### Held back (not uploaded)
- `data/dcbooks_quickstatements_MALAYALAM_CLASSICS_EXCLUDED.txt` — 119 translated classics (1984, Alchemist, Sapiens…). If uploaded later, link each to its existing **work** via `P629` (edition→work).
- `data/dcbooks_quickstatements_ENGLISH.txt` — 311 English-language titles, need title-review for duplicates first.

## NEXT TASK — keralabookstore.com 🔜
Scrape **27,804** Malayalam books (richer than dcbookstore: Malayalam-script titles AND Malayalam author names).
- Scraper: **`scrape_keralabookstore_full.py`** (in repo root). Reads schema.org Book microdata → builds `keralabookstore_wikidata.xlsx`.
- **Must run locally** — the site returns HTTP 429 to datacenter IPs (and throttles fast crawls). Resumable; backs off on 429.
  ```bash
  python3 scrape_keralabookstore_full.py 0 50     # test first
  python3 scrape_keralabookstore_full.py          # full run (hours; run overnight)
  ```
- 22-book sample already scraped: `data/keralabookstore_wikidata_sample.xlsx`.
- **After scraping, before upload:** (1) **dedup by ISBN** against Wikidata (heavy overlap with DC Books + others), (2) map each publisher → QID for `P123` (multi-publisher, unlike DC), (3) generate book-edition QuickStatements in the same model as DC Books.

## Property cheat-sheet (edition model)
`P31`=Q57933693 book edition · `P2093` author (string) · `P212` ISBN-13 (hyphenated) · `P957` ISBN-10 · `P407` language (Malayalam Q36236 / English Q1860) · `P577` date · `P1104` pages · `P393` edition no. · `P437` format (Paperback Q193934 / Hardcover Q193955) · `P629` edition→work · S854/P854 reference URL.
