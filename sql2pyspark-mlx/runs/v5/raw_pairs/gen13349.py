from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("t.channel_values"),
        F.col("t.total_amount"),
    )
)
