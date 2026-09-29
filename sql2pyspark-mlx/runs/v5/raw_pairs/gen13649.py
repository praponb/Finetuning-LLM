from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("store_id") == 39)
    .groupBy("store_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "inner")
    .filter(F.col("store_summary.amount_range") >= 66)
    .select(F.col("stores.region"), F.col("store_summary.amount_range"))
)
