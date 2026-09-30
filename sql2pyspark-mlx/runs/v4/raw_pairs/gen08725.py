from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "left")
    .filter(F.col("O.min_shipping_fee") >= 17)
    .select(F.col("s.region"), F.col("s.city"), F.col("O.min_shipping_fee"))
)
