from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"), F.col("c.city"))
    .agg(
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
        F.sum(F.when(F.col("s.country") == "JP", 1).otherwise(0)).alias("jp_count"),
    )
    .orderBy(F.col("city").desc())
)
