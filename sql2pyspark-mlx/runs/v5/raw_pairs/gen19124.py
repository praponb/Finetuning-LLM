from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 15100, "high").when(F.col("c.credit_limit") >= 3800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.count(F.when(F.col("s.country") == "UK", 1)).alias("uk_count"),
        F.max(F.col("s.started_at")).alias("latest_started_at"),
    )
)
