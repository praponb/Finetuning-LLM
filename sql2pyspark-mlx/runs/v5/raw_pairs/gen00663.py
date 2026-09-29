from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.countDistinct(F.col("s.customer_id")).alias("unique_customers"),
        F.count(F.when(F.col("s.browser") == "Firefox", 1)).alias("firefox_count"),
    )
)
