from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())

base = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "London", "Chiang Mai"]))
    .select(
        "*",
        F.when(F.col("budget") >= 4006000, "High").when(F.col("budget") >= 2053000, "Medium").otherwise("Low").alias("budget_band"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

filtered = (
    base
    .filter(F.col("location") == "London")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_band",
        "location_filled",
        F.round((F.col("budget") * 1.15), 2).alias("budget_scaled"),
    )
)

matched_keys = (
    filtered
    .select("dept_name", "id")
    .intersectAll(
        df_departments
        .filter(F.col("location") == "Chiang Mai")
        .select("dept_name", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.col("dept_name") != "Legal")
    .select(
        "dept_name",
        "location",
        "budget",
        "location_filled",
        F.max("budget").over(w).alias("location_max_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
    )
)

enriched = (
    metrics
    .select(
        "budget",
        "location_max_budget",
        "budget_share",
        F.percent_rank().over(w3).alias("budget_percent_rank"),
        F.sum("budget_share").over(w).alias("location_sum_budget_share"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("budget_percent_rank") >= 0.72, "PREMIUM").otherwise("STANDARD").alias("budget_percent_rank_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.percentile_approx("budget_percent_rank", 0.95).alias("budget_percent_rank_p95"),
        F.round(F.avg("location_sum_budget_share"), 2).alias("avg_location_sum_budget_share"),
    )
    .filter(F.col("avg_location_sum_budget_share") < 1000000)
)
