from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("order_date") >= "2024-03-31")
    .groupBy("store_id")
    .agg(F.min("order_date").alias("first_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("ss.first_order_date"),
    )
)
