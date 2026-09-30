from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(
        F.when(F.col("c.age") >= 71, "high").when(F.col("c.age") >= 41, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.max(F.col("s.end_date")).alias("last_end_date"),
        F.round(((100 * F.sum(F.when(F.col("s.plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
)
