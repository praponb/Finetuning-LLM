from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.store_name"), F.col("s.city"), F.col("ss.latest_order_date"))
    .orderBy("latest_order_date")
)
