from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.total_shipping_fee") >= 200)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("t.total_shipping_fee"),
    )
    .orderBy(F.col("city").desc())
)
