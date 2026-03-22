#!/usr/bin/env python3
"""
Kerala Book Store Scraper — DC Books Malayalam titles
=====================================================
Scrapes keralabookstore.com for DC Books titles to get:
- Malayalam script book titles
- Malayalam script author names
- Malayalam category names
- ISBN

Handles the math CAPTCHA automatically, then scrapes all DC Books listings.
Merges with existing dcbooks_wikidata.xlsx by ISBN match.

Run locally (site blocks cloud IPs):
  python3 scrape_keralabookstore.py
  python3 scrape_keralabookstore.py --merge data/dcbooks_wikidata.xlsx
"""

import requests, re, time, json, argparse, os, operator
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ml,en;q=0.9",
}
BASE = "https://keralabookstore.com"
DELAY = 1.5
PROGRESS_FILE = "kbs_progress.json"
OUTPUT_FILE = "keralabookstore_dcbooks.json"

OPS = {"+": operator.add, "-": operator.sub, "*": operator.mul, "×": operator.mul}


# ── CAPTCHA solver ─────────────────────────────────────────────────────

def solve_captcha(session):
    """Fetch the CAPTCHA page, solve the math problem, and POST the answer."""
    print("Solving CAPTCHA...")
    resp = session.get(BASE, headers=HEADERS, timeout=60)
    soup = BeautifulSoup(resp.text, "lxml")

    # Find the math problem, e.g. "6 - 5 = ?"
    text = soup.get_text()
    match = re.search(r"(\d+)\s*([+\-*×])\s*(\d+)\s*=\s*\?", text)
    if not match:
        # No captcha needed, already in
        if "Solve" not in text and "captcha" not in text.lower():
            print("  No CAPTCHA detected — already accessible")
            return True
        print("  Could not find math problem in page")
        print(f"  Page title: {soup.title.string if soup.title else 'none'}")
        return False

    a, op, b = int(match.group(1)), match.group(2), int(match.group(3))
    answer = OPS[op](a, b)
    print(f"  Problem: {a} {op} {b} = {answer}")

    # POST the answer
    resp2 = session.post(
        f"{BASE}/validateCaptcha",
        data={"captchaAnswer": str(answer)},
        headers={**HEADERS, "Referer": BASE},
        timeout=60,
        allow_redirects=True,
    )

    # Check if we got through
    if "captcha" in resp2.text.lower() and "Solve" in resp2.text:
        print("  CAPTCHA failed — retrying...")
        return solve_captcha(session)

    print(f"  CAPTCHA solved! Status: {resp2.status_code}")
    return True


# ── Page fetching ──────────────────────────────────────────────────────

def get_soup(url, session):
    time.sleep(DELAY)
    resp = session.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    # Check if we hit captcha again
    if "captcha" in resp.text.lower() and "Solve" in resp.text:
        print("  Hit CAPTCHA again, re-solving...")
        solve_captcha(session)
        time.sleep(DELAY)
        resp = session.get(url, headers=HEADERS, timeout=60)

    return BeautifulSoup(resp.text, "lxml"), resp.text


# ── Scraping ───────────────────────────────────────────────────────────

def scrape_book_page(url, session):
    """Scrape a single book page for Malayalam metadata."""
    try:
        soup, raw = get_soup(url, session)
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None

    book = {"url": url}

    # The <title> tag has structured metadata:
    # "buy the book {ML_TITLE} written by {ML_AUTHOR} in category {ML_CAT}, ISBN {ISBN}, Published by {PUB}"
    title_tag = soup.title.string if soup.title else ""
    if title_tag:
        m = re.search(r"buy the book (.+?) written by", title_tag)
        if m:
            book["title_ml"] = m.group(1).strip()

        m = re.search(r"written by (.+?) in category", title_tag)
        if m:
            book["author_ml"] = m.group(1).strip()

        m = re.search(r"in category (.+?),", title_tag)
        if m:
            book["category_ml"] = m.group(1).strip()

        m = re.search(r"ISBN (\d{10,13})", title_tag)
        if m:
            book["isbn"] = m.group(1)

        m = re.search(r"Published by (.+?)(?:\s+from|\s*$)", title_tag)
        if m:
            book["publisher"] = m.group(1).strip()

    # Also extract from page body
    page_text = soup.get_text(separator="\n")

    if "isbn" not in book:
        m = re.search(r"ISBN[:\s]*(\d{10,13})", page_text, re.IGNORECASE)
        if m:
            book["isbn"] = m.group(1)

    # Book ID from URL
    m = re.search(r"/book/[^/]+/(\d+)", url)
    if m:
        book["book_id"] = m.group(1)

    # Malayalam description — longest Malayalam text block
    ml_chunks = re.findall(r"[\u0D00-\u0D7F][\u0D00-\u0D7F\s\u200C\u200D.,;!?()]+", page_text)
    if ml_chunks:
        longest = max(ml_chunks, key=len)
        if len(longest.strip()) > 20:
            book["description_ml"] = longest.strip()[:500]

    return book if (book.get("title_ml") or book.get("isbn")) else None


def scrape_listing_page(soup):
    """Extract book links from a search results / listing page."""
    links = []
    for a in soup.select("a[href*='/book/']"):
        href = a.get("href", "")
        if "/book/" in href:
            if href.startswith("/"):
                href = BASE + href
            if href not in links:
                links.append(href)
    return links


def find_pagination(soup):
    """Find the total number of pages from pagination."""
    max_page = 1
    # Look for pagination links
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        for pat in [r"page=(\d+)", r"pageNo=(\d+)", r"start=(\d+)"]:
            m = re.search(pat, href)
            if m:
                val = int(m.group(1))
                # For start= param, convert to page number
                if "start=" in pat:
                    val = val // 20 + 1
                max_page = max(max_page, val)
        if text.isdigit():
            max_page = max(max_page, int(text))
    return max_page


def scrape_all_dc_books(session):
    """Scrape all DC Books listings."""
    all_books = []
    seen_urls = set()

    # Step 1: Load the publisher search page
    url = f"{BASE}/saved-search.do?publisher=DC+Books"
    print(f"Fetching DC Books listing: {url}")
    soup, raw = get_soup(url, session)

    # Debug: show what we got
    title = soup.title.string if soup.title else "no title"
    print(f"  Page title: {title}")

    # Check if we still hit captcha
    if "captcha" in raw.lower() or "Solve" in raw:
        print("  Still showing CAPTCHA after solving. Retrying...")
        solve_captcha(session)
        soup, raw = get_soup(url, session)
        title = soup.title.string if soup.title else "no title"
        print(f"  Page title after retry: {title}")

    # Find book links
    book_links = scrape_listing_page(soup)
    print(f"  Found {len(book_links)} book links on page 1")

    # Show some raw HTML for debugging if no links found
    if not book_links:
        print("  DEBUG: Looking for any links on page...")
        all_links = soup.select("a[href]")
        print(f"  Total links on page: {len(all_links)}")
        for a in all_links[:20]:
            href = a.get("href", "")
            text = a.get_text(strip=True)[:50]
            if text:
                print(f"    {href[:60]:60s} | {text}")

        # Try alternative URL patterns
        alt_urls = [
            f"{BASE}/saved-search.do?publisher=DC+Books&page=1",
            f"{BASE}/saved-search.do?publisher=DC%20Books",
            f"{BASE}/books-by-publisher/dc-books/",
            f"{BASE}/publisher/DC+Books",
        ]
        for alt in alt_urls:
            print(f"\n  Trying: {alt}")
            try:
                soup2, raw2 = get_soup(alt, session)
                links2 = scrape_listing_page(soup2)
                if links2:
                    book_links = links2
                    print(f"  Found {len(links2)} book links!")
                    break
                else:
                    t = soup2.title.string if soup2.title else "?"
                    print(f"  Title: {t}, links: 0")
            except Exception as e:
                print(f"  Error: {e}")

    # Check pagination
    max_page = find_pagination(soup)
    if max_page > 1:
        print(f"  Pagination detected: {max_page} pages")
        for pg in range(2, max_page + 1):
            for param in [f"page={pg}", f"pageNo={pg}", f"start={(pg-1)*20}"]:
                pg_url = f"{BASE}/saved-search.do?publisher=DC+Books&{param}"
                try:
                    soup_pg, _ = get_soup(pg_url, session)
                    new_links = scrape_listing_page(soup_pg)
                    new_count = 0
                    for lnk in new_links:
                        if lnk not in book_links:
                            book_links.append(lnk)
                            new_count += 1
                    if new_count > 0:
                        print(f"  Page {pg}: +{new_count} links (total: {len(book_links)})")
                        break
                except:
                    continue

    # Step 2: Scrape individual book pages
    print(f"\nScraping {len(book_links)} individual book pages...")
    for i, link in enumerate(book_links):
        if link in seen_urls:
            continue
        seen_urls.add(link)

        book = scrape_book_page(link, session)
        if book:
            all_books.append(book)

        if (i + 1) % 50 == 0:
            save_progress(all_books, seen_urls)
            print(f"  {i+1}/{len(book_links)} scraped, {len(all_books)} books collected")

    return all_books


def save_progress(books, seen_urls):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"books": books, "scraped_urls": list(seen_urls)}, f, ensure_ascii=False)


def merge_with_excel(books, excel_path):
    """Merge scraped Malayalam data into existing Excel by ISBN."""
    try:
        from openpyxl import load_workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    except ImportError:
        print("Install openpyxl: pip install openpyxl")
        return

    isbn_map = {}
    for b in books:
        isbn = b.get("isbn", "")
        if isbn:
            isbn_map[isbn] = b

    print(f"\nMerging {len(isbn_map)} ISBN-matched books into {excel_path}...")

    wb = load_workbook(excel_path)
    ws = wb["Wikidata Upload"]

    headers = {ws.cell(1, c).value: c for c in range(1, ws.max_column + 1)}
    col_isbn13 = headers.get("P212 (ISBN-13)")
    col_isbn10 = headers.get("P957 (ISBN-10)")
    col_label_ml = headers.get("Label (ml)")
    col_desc_ml = headers.get("Description (ml)")

    matched = 0
    ml_updated = 0
    for r in range(2, ws.max_row + 1):
        isbn13 = str(ws.cell(r, col_isbn13).value or "").strip()
        isbn10 = str(ws.cell(r, col_isbn10).value or "").strip()

        kbs_book = isbn_map.get(isbn13) or isbn_map.get(isbn10)
        if not kbs_book:
            continue
        matched += 1

        ml_title = kbs_book.get("title_ml", "")
        if ml_title and any("\u0D00" <= c <= "\u0D7F" for c in ml_title):
            ws.cell(r, col_label_ml).value = ml_title
            ml_updated += 1

        ml_desc = kbs_book.get("description_ml", "")
        if ml_desc and len(ml_desc) > 20 and col_desc_ml:
            existing = ws.cell(r, col_desc_ml).value or ""
            if not existing or len(ml_desc) > len(existing):
                ws.cell(r, col_desc_ml).value = ml_desc

    wb.save(excel_path)
    print(f"Matched: {matched}/{ws.max_row - 1} books by ISBN")
    print(f"Malayalam titles updated: {ml_updated}")


def main():
    parser = argparse.ArgumentParser(description="Scrape keralabookstore.com DC Books")
    parser.add_argument("--merge", default="", help="Excel file to merge Malayalam titles into")
    args = parser.parse_args()

    session = requests.Session()

    # Step 1: Solve CAPTCHA
    if not solve_captcha(session):
        print("Failed to solve CAPTCHA. Exiting.")
        return

    # Step 2: Scrape
    books = scrape_all_dc_books(session)

    # Step 3: Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(books, f, ensure_ascii=False, indent=1)

    # Summary
    with_ml = sum(1 for b in books if b.get("title_ml") and
                  any("\u0D00" <= c <= "\u0D7F" for c in b.get("title_ml", "")))
    with_isbn = sum(1 for b in books if b.get("isbn"))
    print(f"\nSaved {len(books)} books to {OUTPUT_FILE}")
    print(f"With Malayalam title: {with_ml}")
    print(f"With ISBN: {with_isbn}")

    for b in books[:10]:
        print(f"  {b.get('title_ml','?'):40s} | {b.get('isbn','')}")

    # Step 4: Merge
    if args.merge and os.path.exists(args.merge):
        merge_with_excel(books, args.merge)


if __name__ == "__main__":
    main()
