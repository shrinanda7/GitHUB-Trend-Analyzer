"""
GitHub Trend Analyzer — Big Data Analysis with Apache Spark (Windows Friendly)
----------------------------------------------------------------------------
This script performs a full-scale analytical processing of GitHub repository 
data using Apache Spark (PySpark) DataFrames and Window functions.

NOTE: This version uses a hybrid-write approach to ensure compatibility with 
Windows machines without a full Hadoop/winutils setup. All analysis is 
performed in Spark, while the final small-batch write is handled via local IO.
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("GitHubTrendAnalyzer") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__name__))
REPOS_JSON = os.path.join(BASE_DIR, "repos.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "spark_output")

def save_spark_df(df, folder_name):

    target_path = os.path.join(OUTPUT_DIR, folder_name)
    os.makedirs(target_path, exist_ok=True)
    
    # Collect results to driver 
    records = df.collect()
    
    # Write to a part file to mimic Spark output structure
    output_file = os.path.join(target_path, "part-00000.json")
    with open(output_file, "w", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row.asDict()) + "\n")
    print(f"  Saved {len(records)} rows to {folder_name}/part-00000.json")

print(f"Spark Session Started. Reading data from: {REPOS_JSON}")

df = spark.read.json(REPOS_JSON)

# Cast stars and years to proper types
df = df.withColumn("stars", F.col("stars").cast("long")) \
       .withColumn("year", F.col("created_year").cast("int")) \
       .filter(F.col("language").isNotNull() & F.col("year").isNotNull())

print(f"Total records loaded into Spark: {df.count()}")

# 3. Analysis 1: Repo Count per Language per Year
print("\nPerforming Analysis 1: Language Trends (Spark Aggregation)...")
lang_year_counts = df.groupBy("language", "year") \
    .agg(
        F.count("id").alias("repo_count"),
        F.sum("stars").alias("stars_sum")
    ) \
    .orderBy("year", "language")

save_spark_df(lang_year_counts, "lang_year_counts")

# 4. Analysis 2: Topic Trends (flatMap equivalent)
print("\nPerforming Analysis 2: Topic Trends (Spark Explode)...")
topic_counts = df.withColumn("topic", F.explode(F.col("topics"))) \
    .groupBy("topic", "year") \
    .count() \
    .filter("count >= 5") \
    .orderBy(F.desc("count"))

save_spark_df(topic_counts, "topic_counts")

# 5. Analysis 3: Average Stars per Language per Year
print("\nPerforming Analysis 3: Star Averages (Spark GroupBy)...")
star_averages = df.groupBy("language", "year") \
    .agg(F.avg("stars").alias("avg_stars")) \
    .orderBy(F.desc("avg_stars"))

save_spark_df(star_averages, "star_averages")

# 6. Analysis 4: Top Languages Ranked per Year
print("\nPerforming Analysis 4: Language Rankings (Spark Windowing)...")
windowSpec = Window.partitionBy("year").orderBy(F.desc("repo_count"))

top_languages = lang_year_counts.withColumn("rank", F.rank().over(windowSpec)) \
    .orderBy("year", "rank")

save_spark_df(top_languages, "top_languages")

# 7. Analysis 5: Year-over-Year Growth Rate (Window Functions)
print("\nPerforming Analysis 5: Growth Rates (Spark LAG)...")
year_totals = lang_year_counts.groupby("year") \
    .agg(F.sum("stars_sum").alias("year_star_total"))

growth_df = lang_year_counts.join(year_totals, "year") \
    .withColumn("share_pct_stars", (F.col("stars_sum") / F.col("year_star_total")) * 100)

# Window for LAG
langWindow = Window.partitionBy("language").orderBy("year")

growth_rates = growth_df \
    .withColumn("prev_share_pct", F.lag("share_pct_stars").over(langWindow)) \
    .withColumn("growth_stars_pct", 
                F.round(((F.col("share_pct_stars") - F.col("prev_share_pct")) / F.col("prev_share_pct")) * 100, 2)) \
    .filter(F.col("growth_stars_pct").isNotNull()) \
    .orderBy(F.desc("year"), F.desc("growth_stars_pct"))

save_spark_df(growth_rates, "growth_rates")

# 8. Analysis 6: Top 20 Most Starred Repositories
print("\nPerforming Analysis 6: Top Repositories (Spark Select/Order)...")
top_repos = df.select("name", "language", "year", "stars", "forks") \
    .orderBy(F.desc("stars")) \
    .limit(20)

save_spark_df(top_repos, "top_repos")

# 9. Analysis 7: Language Market Share per Year
print("\nPerforming Analysis 7: Market Share (Spark SQL)...")
market_share = growth_df.select("language", "year", "share_pct_stars") \
    .orderBy("year", F.desc("share_pct_stars"))

save_spark_df(market_share, "market_share")

print("\n[SUCCESS] All analyses completed using Spark Engine.")
spark.stop()
