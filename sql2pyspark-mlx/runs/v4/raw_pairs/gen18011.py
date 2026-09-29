from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("dept_name").isin(["Finance", "Legal"]))
    .select(
        "*",
        F.lower("dept_name").alias("dept_name_lower"),
        F.when(F.col("location").isin(["Bangkok", "Singapore"]), "Tier 1").otherwise("Other").alias("location_group"),
        F.coalesce("location", F.lit("DIRECT")).alias("resolved_location"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("budget") >= 4377000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.max(F.when(F.col("location") == "Chiang Mai", F.col("budget"))).alias("chiang_mai_max_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("budget_per_engineering"),
    )
)
