"""
PHASE 3 + 4 — GitHub Trend Analysis (Pandas-based, Windows-compatible)
------------------------------------------------------------------------
This script reads repos.json (exported from MongoDB) and computes all
analyses, producing the same output/ folder structure that app.py reads.

Why pandas instead of PySpark?
  PySpark on Windows requires winutils.exe + HADOOP_HOME which is a
  heavyweight setup. Pandas handles 64 k rows in seconds with identical
  analytical logic.

Run after fetcher.py has populated MongoDB and you have repos.json:
  python spark_analysis.py

Output folders created:
  output/lang_year_counts/   — repo count per language per year
  output/topic_counts/       — trending topic counts per year
  output/star_averages/      — average stars per language per year
  output/growth_rates/       — year-over-year growth % per language
  output/top_languages/      — top languages ranked per year
  output/top_repos/          — top 20 most starred repos
  output/market_share/       — language market share % per year
"""

import json
import os
import pandas as pd

# Paths relative to this script (works no matter what folder you run from).
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPOS_JSON = os.path.join(BASE_DIR, "repos.json")

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def save_output(df: pd.DataFrame, folder: str):
    """
    Save DataFrame as newline-delimited JSON (one object per line)
    into output/<folder>/part-00000.json  — same format app.py expects.
    """
    out_dir = os.path.join(BASE_DIR, "output", folder)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "part-00000.json")
    import math
    records = df.to_dict(orient="records")
    with open(out_path, "w", encoding="utf-8") as f:
        for record in records:
            # Replace NaN/Inf with None so JSON.parse() on the frontend doesn't crash
            clean = {k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
                     for k, v in record.items()}
            f.write(json.dumps(clean) + "\n")
    print(f"  Saved {len(records):,} rows -> output/{folder}/part-00000.json")


# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────

print("=" * 60)
print("GitHub Trend Analyzer — Analysis Engine")
print("=" * 60)

print(f"\nLoading {REPOS_JSON} …")
rows = []
with open(REPOS_JSON, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

df = pd.DataFrame(rows)
if "stars" in df.columns:
    df["stars"] = pd.to_numeric(df["stars"], errors="coerce").fillna(0)
print(f"Total repos loaded: {len(df):,}")
print(f"Columns: {list(df.columns)}")
print(f"Years in data: {sorted(df['created_year'].dropna().unique().astype(int).tolist())}")
print(f"Languages (sample): {df['language'].dropna().unique()[:10].tolist()}")


# ─────────────────────────────────────────────
# ANALYSIS 1 — Repo count per language per year
# (RDD-style: map → groupBy → sum)
# ─────────────────────────────────────────────

print("\n" + "=" * 50)
print("Analysis 1: Repo count per language per year")
print("=" * 50)

# Repo counts are often ~1k per language/year (GitHub Search API cap). Include
# stars_sum so the dashboard can plot star totals, which still vary under the cap.
lang_year = (
    df[df["language"].notna()]
    .groupby(["language", "created_year"], as_index=False)
    .agg(repo_count=("language", "count"), stars_sum=("stars", "sum"))
    .rename(columns={"created_year": "year"})
    .sort_values(["year", "language"])
)
lang_year["year"] = lang_year["year"].astype(int)
print(lang_year.to_string(index=False))
save_output(lang_year, "lang_year_counts")


# ─────────────────────────────────────────────
# ANALYSIS 2 — Topic trends (flatMap equivalent)
# ─────────────────────────────────────────────

print("\n" + "=" * 50)
print("Analysis 2: Topic trends (flatMap)")
print("=" * 50)

topic_rows = []
for _, row in df.iterrows():
    topics = row.get("topics")
    year   = row.get("created_year")
    if not topics or not year:
        continue
    if not isinstance(topics, list):
        continue
    for topic in topics:
        if topic:
            topic_rows.append({"topic": str(topic), "year": int(year)})

if topic_rows:
    topic_df = pd.DataFrame(topic_rows)
    topic_counts = (
        topic_df.groupby(["topic", "year"])
        .size()
        .reset_index(name="count")
    )
    # Only keep topics that appear ≥5 times (same filter as original RDD code)
    topic_counts = topic_counts[topic_counts["count"] >= 5]
    topic_counts = topic_counts.sort_values("count", ascending=False)
    print(topic_counts.head(20).to_string(index=False))
    save_output(topic_counts, "topic_counts")
else:
    print("  No topic data found — saving empty file.")
    save_output(pd.DataFrame(columns=["topic", "year", "count"]), "topic_counts")


# ─────────────────────────────────────────────
# ANALYSIS 3 — Average stars per language per year
# ─────────────────────────────────────────────

print("\n" + "=" * 50)
print("Analysis 3: Average stars per language per year")
print("=" * 50)

star_df = df[df["language"].notna() & df["stars"].notna()].copy()
star_df["stars"] = star_df["stars"].astype(int)

star_avgs = (
    star_df.groupby(["language", "created_year"])
    .agg(
        total_stars=("stars", "sum"),
        repo_count =("stars", "count"),
    )
    .reset_index()
    .rename(columns={"created_year": "year"})
)
star_avgs["year"]      = star_avgs["year"].astype(int)
star_avgs["avg_stars"] = (star_avgs["total_stars"] / star_avgs["repo_count"]).round(2)
star_avgs = star_avgs.sort_values("avg_stars", ascending=False)
print(star_avgs.head(20).to_string(index=False))
save_output(star_avgs, "star_averages")


# ─────────────────────────────────────────────
# ANALYSIS 4 — Top languages per year (SQL-style)
# ─────────────────────────────────────────────

print("\n" + "=" * 50)
print("Analysis 4: Top languages per year (SQL-style)")
print("=" * 50)

top_langs = (
    df[df["language"].notna()]
    .groupby(["language", "created_year"])
    .agg(
        repo_count  =("id",    "count"),
        total_stars =("stars", "sum"),
    )
    .reset_index()
    .rename(columns={"created_year": "year"})
)
top_langs["year"]      = top_langs["year"].astype(int)
top_langs["avg_stars"] = (top_langs["total_stars"] / top_langs["repo_count"]).round(0)
top_langs = top_langs.sort_values(["year", "repo_count"], ascending=[True, False])
print(top_langs.head(30).to_string(index=False))
save_output(top_langs, "top_languages")


# ─────────────────────────────────────────────
# ANALYSIS 5 — Year-over-year growth rate (window function)
# ─────────────────────────────────────────────

print("\n" + "=" * 50)
print("Analysis 5: Year-over-year growth rate")
print("=" * 50)

yearly_counts = (
    df[df["language"].notna()]
    .groupby(["language", "created_year"], as_index=False)
    .agg(repo_count=("language", "count"), stars_sum=("stars", "sum"))
    .rename(columns={"created_year": "year"})
    .sort_values(["language", "year"])
)
yearly_counts["year"] = yearly_counts["year"].astype(int)

# LAG equivalent — shift within each language group
yearly_counts["prev_year_count"] = (
    yearly_counts.groupby("language")["repo_count"].shift(1)
)
yearly_counts["prev_year_stars"] = (
    yearly_counts.groupby("language")["stars_sum"].shift(1)
)
yearly_counts["growth_pct"] = yearly_counts.apply(
    lambda r: round(
        ((r["repo_count"] - r["prev_year_count"]) / r["prev_year_count"]) * 100, 2
    )
    if pd.notna(r["prev_year_count"]) and r["prev_year_count"] > 0
    else None,
    axis=1,
)
# YoY change in total stars — meaningful when repo counts hit the ~1k/search cap.
yearly_counts["growth_stars_pct"] = yearly_counts.apply(
    lambda r: round(
        ((r["stars_sum"] - r["prev_year_stars"]) / r["prev_year_stars"]) * 100, 2
    )
    if pd.notna(r["prev_year_stars"]) and r["prev_year_stars"] > 0
    else None,
    axis=1,
)
growth = yearly_counts.sort_values(["year", "growth_pct"], ascending=[False, False])
print(growth.head(30).to_string(index=False))
save_output(growth, "growth_rates")


# ─────────────────────────────────────────────
# ANALYSIS 6 — Top 20 most starred repos
# ─────────────────────────────────────────────

print("\n" + "=" * 50)
print("Analysis 6: Top 20 most starred repos")
print("=" * 50)

top_repos = (
    df[["name", "language", "created_year", "stars", "forks"]]
    .copy()
    .rename(columns={"created_year": "year"})
    .sort_values("stars", ascending=False)
    .head(20)
)
top_repos["year"]  = top_repos["year"].fillna(0).astype(int)
top_repos["stars"] = top_repos["stars"].fillna(0).astype(int)
top_repos["forks"] = top_repos["forks"].fillna(0).astype(int)
print(top_repos.to_string(index=False))
save_output(top_repos, "top_repos")


# ─────────────────────────────────────────────
# ANALYSIS 7 — Language market share per year
# ─────────────────────────────────────────────

print("\n" + "=" * 50)
print("Analysis 7: Language market share per year")
print("=" * 50)

lang_counts = (
    df[df["language"].notna()]
    .groupby(["language", "created_year"], as_index=False)
    .agg(lang_count=("language", "count"), star_sum=("stars", "sum"))
    .rename(columns={"created_year": "year"})
)
year_totals = (
    lang_counts.groupby("year")["lang_count"]
    .sum()
    .reset_index(name="year_total")
)
year_star_totals = (
    lang_counts.groupby("year")["star_sum"]
    .sum()
    .reset_index(name="year_star_total")
)
market_share = lang_counts.merge(year_totals, on="year").merge(year_star_totals, on="year")
market_share["year"]      = market_share["year"].astype(int)
market_share["share_pct"] = (
    (market_share["lang_count"] / market_share["year_total"]) * 100
).round(2)
market_share["share_pct_stars"] = (
    (market_share["star_sum"] / market_share["year_star_total"]) * 100
).round(2)
market_share = market_share.sort_values(["year", "share_pct"], ascending=[True, False])
print(market_share.head(40).to_string(index=False))
save_output(market_share, "market_share")


# ─────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────

print("\n" + "=" * 60)
print("All analyses complete! Files saved to output/ folder:")
for name in ["lang_year_counts", "topic_counts", "star_averages",
             "growth_rates", "top_languages", "top_repos", "market_share"]:
    path = os.path.join("output", name, "part-00000.json")
    size = os.path.getsize(path) if os.path.exists(path) else 0
    print(f"  output/{name}/part-00000.json  ({size:,} bytes)")
print("\nNext step: run  python app.py  to start the dashboard.")
print("=" * 60)