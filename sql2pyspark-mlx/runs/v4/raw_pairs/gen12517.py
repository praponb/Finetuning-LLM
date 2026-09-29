from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.customer_id").isin([4, 7, 32]))
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        F.count(F.when(F.col("o.status") == "PENDING", 1)).alias("pending_count"),
        F.min(F.col("o.amount")).alias("min_amount"),
    )
)
