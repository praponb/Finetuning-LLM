from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Berlin"]))
    .select(
        "*",
        F.round((F.col("budget") * 0.9), 2).alias("budget_adjusted"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("budget") >= 4299000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.max("budget_adjusted").alias("max_budget_adjusted"),
        F.avg(F.when(F.col("location") == "New York", F.col("budget_adjusted"))).alias("new_york_avg_budget_adjusted"),
    )
    .filter(F.col("max_budget_adjusted") > 0)
)
