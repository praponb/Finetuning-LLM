from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .filter(F.col("obs.amount_range") > 22)
    .select(F.col("s.id"), F.col("s.region"), F.col("obs.amount_range"))
)
