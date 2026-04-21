"""
PHASE 5 — Flask Dashboard Backend
-----------------------------------
This is a lightweight web server that:
  1. Serves the dashboard HTML page (index.html)
  2. Reads Spark output JSON files
  3. Exposes API endpoints that the dashboard calls to get chart data

Run this AFTER spark_analysis.py is done:
  python app.py

Then open your browser at:
  http://localhost:5000
"""

from flask import Flask, jsonify, render_template
import json
import os
import glob
import requests

# Always resolve paths next to this file — APIs must work even if you run
# `python app.py` from a parent folder (otherwise `output/` is read from CWD
# and the dashboard loads empty).
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"))


# ─────────────────────────────────────────────
# HELPER: Read Spark JSON output files
# ─────────────────────────────────────────────

def read_spark_output(folder_name):
    """
    Spark saves output as multiple part-*.json files inside a folder.
    This function reads ALL those files and combines them into one list.
    Each line in the file is one JSON object (one row of data).
    """
    results = []
    out_root = os.path.join(BASE_DIR, "output", folder_name)
    pattern = os.path.join(out_root, "part-*.json")
    files   = glob.glob(pattern)

    if not files:
        # Try alternate extension
        pattern = os.path.join(out_root, "*.json")
        files   = glob.glob(pattern)

    for filepath in files:
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

    return results


# ─────────────────────────────────────────────
# ROUTE: Main dashboard page
# ─────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the dashboard HTML page."""
    return render_template("index.html")


# ─────────────────────────────────────────────
# API ROUTES: Called by dashboard JavaScript
# ─────────────────────────────────────────────

@app.route("/api/lang-trends")
def lang_trends():
    """
    Returns repo count per language per year.
    Used for: Line chart showing growth over time.
    Example response: [{"language":"Python","year":2020,"repo_count":4200}, ...]
    """
    data = read_spark_output("lang_year_counts")
    return jsonify(sorted(data, key=lambda x: (x.get("year", 0), x.get("language", ""))))


@app.route("/api/star-averages")
def star_averages():
    """
    Returns average stars per language per year.
    Used for: Bar chart showing star popularity.
    """
    data = read_spark_output("star_averages")
    return jsonify(sorted(data, key=lambda x: x.get("year", 0)))


@app.route("/api/growth-rates")
def growth_rates():
    """
    Returns year-over-year growth percentage per language.
    Used for: Growth rate bar chart.
    """
    data = read_spark_output("growth_rates")
    # Only return rows where growth_pct is not null
    filtered = [d for d in data if d.get("growth_pct") is not None]
    return jsonify(sorted(filtered, key=lambda x: x.get("year", 0)))


@app.route("/api/top-languages")
def top_languages():
    """
    Returns languages ranked by repo count per year.
    Used for: Stacked/grouped bar chart.
    """
    data = read_spark_output("top_languages")
    return jsonify(sorted(data, key=lambda x: (x.get("year", 0), -x.get("repo_count", 0))))


@app.route("/api/topic-trends")
def topic_trends():
    """
    Returns trending topic counts per year.
    Used for: Topic cloud / bar chart.
    """
    data = read_spark_output("topic_counts")
    return jsonify(sorted(data, key=lambda x: -x.get("count", 0)))


@app.route("/api/market-share")
def market_share():
    """
    Returns language market share percentage per year.
    Used for: Pie/donut chart.
    """
    data = read_spark_output("market_share")
    return jsonify(sorted(
        data,
        key=lambda x: (x.get("year", 0), -x.get("share_pct_stars", x.get("share_pct", 0))),
    ))


@app.route("/api/top-repos")
def top_repos():
    """
    Returns top 20 most starred repos.
    Used for: Table / horizontal bar chart.
    """
    data = read_spark_output("top_repos")
    return jsonify(sorted(data, key=lambda x: -x.get("stars", 0)))


def _top_language_from_lang_year_counts(lang_data):
    """
    Pick the language with the most presence in the dataset.
    The old logic sorted every (language, year) row by repo_count and took [0];
    many rows tie at ~1000 (search cap), so the "winner" was arbitrary (often "C").
    """
    if not lang_data:
        return "N/A"
    totals = {}
    for d in lang_data:
        lang = d.get("language")
        if not lang:
            continue
        if lang not in totals:
            totals[lang] = {"repos": 0, "stars": 0}
        totals[lang]["repos"] += d.get("repo_count", 0) or 0
        totals[lang]["stars"] += d.get("stars_sum", 0) or 0
    # Prefer total stars (varies under the cap), then total repos.
    best = max(totals.items(), key=lambda kv: (kv[1]["stars"], kv[1]["repos"]))
    return best[0]


@app.route("/api/summary")
def summary():
    """
    Returns summary stats for the header cards.
    """
    lang_data = read_spark_output("lang_year_counts")
    languages = list(set(d["language"] for d in lang_data if "language" in d))
    years     = list(set(d["year"] for d in lang_data if "year" in d))
    total_repos = sum(d.get("repo_count", 0) for d in lang_data)

    return jsonify({
        "total_repos"  : f"{total_repos:,}",
        "languages"    : len(languages),
        "years_covered": f"{min(years)} – {max(years)}" if years else "N/A",
        "top_language" : _top_language_from_lang_year_counts(lang_data),
    })


@app.route("/api/user/<username>")
def user_data(username):
    """
    Fetches real-time user data and repos from GitHub API.
    Used for: User specific data analysis on the dashboard.
    """
    headers = {"Accept": "application/vnd.github.v3+json"}
    
    u_resp = requests.get(f"https://api.github.com/users/{username}", headers=headers)
    if u_resp.status_code != 200:
        return jsonify({"error": "User not found or API rate limit exceeded"}), 404
        
    r_resp = requests.get(f"https://api.github.com/users/{username}/repos?per_page=100&sort=stargazers", headers=headers)
    repos_data = []
    if r_resp.status_code == 200:
        repos_data = r_resp.json()
        
    return jsonify({
        "profile": u_resp.json(),
        "repos": repos_data
    })


# ─────────────────────────────────────────────
# START SERVER
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting GitHub Trend Analyzer Dashboard...")
    print("Open your browser at: http://localhost:5000")
    app.run(debug=True, port=5000)