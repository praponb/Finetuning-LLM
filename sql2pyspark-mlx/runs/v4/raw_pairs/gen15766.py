from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("customer_id").isin([6, 10, 39]))
    .groupBy("store_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.amount_range") > 64)
    .select(F.col("s.id"), F.col("s.region"), F.col("ss.amount_range"))
)
