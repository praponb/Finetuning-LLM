from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.segment").isin(["Consumer", "Corporate", "SMB"]))
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.when(F.col("s.device") == "desktop", 1)).alias("desktop_count"),
        F.countDistinct(F.col("s.customer_id")).alias("unique_customers"),
    )
)
