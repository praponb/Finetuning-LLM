from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("session_count"),
        F.sum(F.when(F.col("s.country") == "JP", 1).otherwise(0)).alias("jp_count"),
    )
)
