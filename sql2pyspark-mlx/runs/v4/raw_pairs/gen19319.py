from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("store_summary"), F.col("stores.id") == F.col("store_summary.store_id"), "left")
    .select(
        F.col("stores.opened_date"),
        F.col("stores.region"),
        F.col("store_summary.channel_values"),
        F.col("store_summary.last_order_date"),
    )
    .orderBy(F.col("region").desc(), "opened_date")
)
