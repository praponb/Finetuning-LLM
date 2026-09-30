from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.country").isin(["UK", "DE"]))
    .groupBy(F.col("c.is_active"))
    .agg(
        F.sum(F.when(F.col("s.browser") == "Edge", 1).otherwise(0)).alias("edge_count"),
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
    )
    .orderBy(F.col("unique_countrys").desc())
)
