from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("customer_id").isin([16, 32, 35, 38]))
    .groupBy("store_id")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("S.channel_list"),
        F.col("S.last_order_date"),
    )
)
