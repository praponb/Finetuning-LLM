from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17500, "high").when(F.col("c.credit_limit") >= 7800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.min(F.col("s.started_at")).alias("min_started_at"))
    .orderBy("credit_limit_bucket", F.col("min_started_at").desc())
)
