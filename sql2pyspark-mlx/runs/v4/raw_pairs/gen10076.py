from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("id", "location")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .rollup(
        "location",
        F.when(F.col("budget") >= 3251000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("budget").alias("avg_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "New York", 1)), F.lit(0)), 2).alias("budget_per_new_york"),
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget"))).alias("singapore_total_budget"),
    )
    .filter(F.col("budget_per_new_york") > 0)
    .orderBy("row_count")
)
