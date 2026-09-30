from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_name").orderBy(F.col("budget_whole").desc())
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("dept_name", "location")
w4 = Window.orderBy(F.col("budget_share").asc_nulls_last())

base = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Engineering"]))
    .withColumn("budget_whole", F.floor("budget"))
)

filtered = (
    base
    .filter(F.col("budget_whole") >= 49)
    .withColumn("clean_location", F.trim("location"))
    .withColumn("location_clean", F.coalesce("location", F.lit("UNKNOWN")))
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") <= 106000), "left_semi")
)

kept_keys = (
    kept
    .select("location")
    .intersect(
        df_departments
        .filter(F.col("budget") <= 3634000)
        .select("location")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.col("dept_name") == "Marketing")
    .withColumn("budget_whole_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w3)), 4))
)

base_departments = (
    enriched
    .filter(F.col("dept_name") != "Engineering")
    .select(
        "*",
        F.lower("location").alias("location_lower"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

with_windows = (
    base_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_whole",
        "clean_location",
        "location_lower",
        "resolved_location",
        F.rank().over(w4).alias("budget_share_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget_whole") >= 85, "HIGH").otherwise("LOW").alias("budget_whole_tier"),
    )
    .agg(
        F.avg(F.when(F.col("location") == "New York", F.col("budget_whole"))).alias("new_york_avg_budget_whole"),
        F.max(F.when(F.col("location") == "Bangkok", F.col("budget_share_rank"))).alias("bangkok_max_budget_share_rank"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_rows"),
    )
    .orderBy(F.col("bangkok_rows").desc_nulls_last())
)
