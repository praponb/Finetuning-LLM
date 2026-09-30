from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.sum(F.when(F.col("o.status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count", F.col("pending_count").desc())
)
