from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.amount_range") > 47)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("ss.total_shipping_fee"),
        F.col("ss.amount_range"),
    )
)
