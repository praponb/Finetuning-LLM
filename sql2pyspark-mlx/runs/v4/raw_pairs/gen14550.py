from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("channel") == "web")
    .groupBy("store_id")
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("obs.min_shipping_fee"))
    .orderBy(F.col("id").desc())
)
