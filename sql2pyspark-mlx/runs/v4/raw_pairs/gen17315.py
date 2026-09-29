from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") >= "2025-10-26") | (F.col("country") == "UK"))
    .groupBy(
        F.when(F.col("age") >= 69, "high").when(F.col("age") >= 39, "mid").otherwise("low").alias("age_bucket"),
        "segment",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.min("signup_date").alias("min_signup_date"),
    )
    .orderBy(F.col("age_bucket").desc(), "segment")
)
