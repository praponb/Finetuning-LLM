from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "New York"]))
    .select(
        "*",
        F.round((F.col("budget") * 100), 2).alias("adjusted_budget"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3322000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "New York", 1)), F.lit(0)), 2).alias("budget_per_new_york"),
        F.percentile_approx("adjusted_budget", [0.25, 0.5, 0.75]).alias("adjusted_budget_quartiles"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_rows"),
    )
    .orderBy("berlin_rows")
)
