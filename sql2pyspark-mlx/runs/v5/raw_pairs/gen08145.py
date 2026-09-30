from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("discount") > 0.26)
    .groupBy("store_id")
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("t"), F.col("stores.id") == F.col("t.store_id"), "inner")
    .select(F.col("stores.sqft"), F.col("stores.city"), F.col("t.avg_shipping_fee"))
    .orderBy("city")
)
