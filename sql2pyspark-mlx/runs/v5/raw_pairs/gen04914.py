from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((~F.col("city").eqNullSafe(F.lit("New York"))) & (F.col("country") == "TH"))
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    valid
    .groupBy(F.when(F.col("age") >= 65, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("age_per_uk"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
)
