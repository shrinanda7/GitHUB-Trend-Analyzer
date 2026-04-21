"""
PHASE 2 — GitHub Data Fetcher
---------------------------------------------------------
Fetches repository data from GitHub Search API by YEAR × LANGUAGE
and stores it in MongoDB (Database: github_trends, Collection: repos).

GitHub Search API caps results at 1000 per query (10 pages × 100).
By splitting each year into per-language queries we maximise coverage.

Expected yield: ~8 languages × 7 years × up to 1000 repos = ~56 000 docs
"""

import requests
import pymongo
import time
import sys

# ─────────────────────────────────────────────
# CONFIGURATION  —  edit only this block
# ─────────────────────────────────────────────

TOKEN = "your_github_token_here"   # ← your GitHub PAT

HEADERS = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

YEARS = [2025, 2026]                     # only fetching 2025 and 2026

LANGUAGES = [
    "Python",
    "JavaScript",
    "TypeScript",
    "Java",
    "Go",
    "Rust",
    "C++",
    "C#",
]

PAGES_PER_QUERY = 10    # GitHub hard-caps at page 10 (1 000 results max)
PER_PAGE        = 100   # max allowed by GitHub API
SLEEP_SECONDS   = 1.2   # polite delay between requests

# ─────────────────────────────────────────────
# MongoDB connection
# ─────────────────────────────────────────────

client     = pymongo.MongoClient("mongodb://localhost:27017/")
db         = client["github_trends"]
collection = db["repos"]

# Unique compound index so re-runs never duplicate records
collection.create_index(
    [("id", pymongo.ASCENDING)],
    unique=True,
    background=True,
)

# ─────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────

def fetch_page(year: int, language: str, page: int) -> list[dict]:
    """Fetch one page of repos for a given year + language. Returns list of docs."""
    date_range = f"{year}-01-01..{year}-12-31"
    q = f"created:{date_range}+language:{language}"
    url = (
        f"https://api.github.com/search/repositories"
        f"?q={q}&sort=stars&order=desc&per_page={PER_PAGE}&page={page}"
    )

    for attempt in range(3):          # simple retry on transient errors
        resp = requests.get(url, headers=HEADERS, timeout=20)

        if resp.status_code == 200:
            items = resp.json().get("items", [])
            docs = []
            for item in items:
                created_at = item.get("created_at", "")
                docs.append({
                    "id":           item["id"],
                    "name":         item.get("full_name"),
                    "language":     item.get("language") or language,
                    "created_year": int(created_at[:4]) if created_at else year,
                    "topics":       item.get("topics", []),
                    "stars":        item.get("stargazers_count", 0),
                    "forks":        item.get("forks_count", 0),
                    "description":  item.get("description", ""),
                    "watchers":     item.get("watchers_count", 0),
                    "open_issues":  item.get("open_issues_count", 0),
                    "size":         item.get("size", 0),
                })
            return docs

        elif resp.status_code == 403:
            # Rate-limit hit — wait and retry
            reset_in = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60)) - int(time.time())
            wait = max(reset_in + 2, 10)
            print(f"    ⚠  Rate-limited. Sleeping {wait}s …", flush=True)
            time.sleep(wait)

        elif resp.status_code == 422:
            # GitHub rejects pages beyond the 1 000-result cap — stop quietly
            return []

        else:
            print(f"    ✗  HTTP {resp.status_code} on attempt {attempt+1}. Retrying…", flush=True)
            time.sleep(5)

    return []   # give up after 3 attempts


def upsert_docs(docs: list[dict]) -> int:
    """Upsert a batch of docs. Returns number successfully written."""
    written = 0
    for d in docs:
        try:
            collection.update_one({"id": d["id"]}, {"$set": d}, upsert=True)
            written += 1
        except Exception as exc:
            print(f"    ✗  Mongo error: {exc}", flush=True)
    return written


# ─────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────

def fetch_data():
    grand_total = 0

    for year in YEARS:
        print(f"\n{'='*50}")
        print(f"YEAR: {year}")
        print(f"{'='*50}", flush=True)
        year_total = 0

        for lang in LANGUAGES:
            print(f"\n  [{lang}] Fetching repos from {year}…", flush=True)
            lang_total = 0

            for page in range(1, PAGES_PER_QUERY + 1):
                docs = fetch_page(year, lang, page)

                if not docs:
                    print(f"    Page {page:2d}: no more results — stopping.", flush=True)
                    break

                written = upsert_docs(docs)
                lang_total  += written
                print(
                    f"    Page {page:2d}: got {len(docs)} repos  |  "
                    f"running total: {lang_total}",
                    flush=True,
                )
                time.sleep(SLEEP_SECONDS)

            year_total  += lang_total
            grand_total += lang_total
            total_in_db  = collection.count_documents({})
            print(
                f"  [{lang}] Saved {lang_total} repos | Total in DB: {total_in_db}",
                flush=True,
            )

        print(f"\n  ── Year {year} done: {year_total} repos saved ──", flush=True)

    print(f"\n{'='*50}")
    print(f"DONE! Total documents in MongoDB: {collection.count_documents({})}")
    print(f"Processed this run:               {grand_total}")
    print(f"{'='*50}", flush=True)


if __name__ == "__main__":
    existing = collection.count_documents({})
    print("Connected to MongoDB successfully.")
    print(f"Database  : github_trends")
    print(f"Collection: repos")
    print(f"Existing docs in collection: {existing}")
    print()

    if existing > 0:
        answer = input("Collection already has data. Continue and upsert? [y/N]: ").strip().lower()
        if answer != "y":
            print("Aborted.")
            sys.exit(0)

    fetch_data()