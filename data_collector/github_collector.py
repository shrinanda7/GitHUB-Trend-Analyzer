import os
import time
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ── Configuration ────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
MONGO_URI    = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

if not GITHUB_TOKEN:
    print("⚠️  Warning: GITHUB_TOKEN not set. You will hit GitHub's 60 req/hr limit fast!")

# ── MongoDB Atlas Connection ─────────────────────────────────────────────────
print(f"🔗 Connecting to MongoDB...")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
try:
    client.admin.command("ping")
    print("✅ Connected to MongoDB successfully!\n")
except Exception as e:
    print(f"❌ Could not connect to MongoDB: {e}")
    exit(1)

db                = client["github_global_analysis"]
repos_collection  = db["repos"]

# Avoid duplicates across multiple runs
repos_collection.create_index("id", unique=True)

# ── GitHub API Setup ─────────────────────────────────────────────────────────
HEADERS = {"Accept": "application/vnd.github.v3+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"

# ── What to collect ──────────────────────────────────────────────────────────
LANGUAGES = ["Python", "JavaScript", "Java", "C++", "Go", "Rust", "TypeScript"]
YEARS     = list(range(2018, 2025))   # 2018 → 2025
PAGES_PER_COMBO = 5                    # 5 pages × 100 results = 500 repos max per (lang, year)

# ── Core Fetch Function ──────────────────────────────────────────────────────
def fetch_repos(language: str, year: int, page: int = 1, per_page: int = 100):
    url   = "https://api.github.com/search/repositories"
    query = f"language:{language} created:{year}-01-01..{year}-12-31"
    params = {"q": query, "sort": "stars", "order": "desc",
              "per_page": per_page, "page": page}

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)

            # Rate limit handling
            if resp.status_code == 403:
                reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait  = max(reset - int(time.time()), 0) + 5
                print(f"  ⏳ Rate limited! Sleeping {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data  = resp.json()
            items = data.get("items", [])
            total = data.get("total_count", 0)
            return items, total

        except requests.exceptions.Timeout:
            print(f"  ⚠️  Timeout on attempt {attempt+1}/3, retrying...")
            time.sleep(5)
        except Exception as e:
            print(f"  ❌ Error: {e}")
            break

    return [], 0


# ── Store to MongoDB ─────────────────────────────────────────────────────────
def store_repos(items: list, language: str, year: int):
    saved = 0
    for item in items:
        doc = {
            "id":           item.get("id"),
            "name":         item.get("name"),
            "full_name":    item.get("full_name"),
            "owner":        item.get("owner", {}).get("login"),
            "html_url":     item.get("html_url"),
            "description":  item.get("description"),
            "language":     language,           # use our search language (more reliable)
            "stars":        item.get("stargazers_count", 0),
            "forks":        item.get("forks_count", 0),
            "watchers":     item.get("watchers_count", 0),
            "open_issues":  item.get("open_issues_count", 0),
            "topics":       item.get("topics", []),
            "created_at":   item.get("created_at"),
            "updated_at":   item.get("updated_at"),
            "year":         year,
            "collected_at": datetime.utcnow().isoformat(),
        }
        try:
            repos_collection.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
            saved += 1
        except Exception as e:
            pass  # Duplicate key errors are expected and safe to skip
    return saved


# ── Main Collection Loop ─────────────────────────────────────────────────────
def main():
    total_saved = 0
    combo_total = len(YEARS) * len(LANGUAGES)
    combo_done  = 0

    print("=" * 60)
    print("🚀 GitHub Global Data Collection Started")
    print(f"   Years     : {YEARS[0]} → {YEARS[-1]}")
    print(f"   Languages : {', '.join(LANGUAGES)}")
    print(f"   Max repos : {combo_total * PAGES_PER_COMBO * 100} (theoretical)")
    print("=" * 60 + "\n")

    for year in YEARS:
        for language in LANGUAGES:
            combo_done += 1
            print(f"[{combo_done}/{combo_total}] Fetching: {language} | {year}")

            for page in range(1, PAGES_PER_COMBO + 1):
                items, total_count = fetch_repos(language, year, page)

                if not items:
                    print(f"  → No more results (page {page}). Moving on.")
                    break

                saved = store_repos(items, language, year)
                total_saved += saved
                print(f"  📄 Page {page}: saved {saved}/{len(items)} | Total so far: {total_saved}")

                # Respect GitHub's secondary rate limit (10 search reqs/min)
                time.sleep(2)

    print("\n" + "=" * 60)
    print(f"✅ Collection complete! Total repos saved: {total_saved}")
    print(f"📦 MongoDB DB  : github_global_analysis")
    print(f"📦 Collection  : repos")
    print("=" * 60)


if __name__ == "__main__":
    main()
