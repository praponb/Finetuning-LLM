from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
    )
)
