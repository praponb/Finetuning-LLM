from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.channel")))).alias("channel_values"),
        F.count(F.col("o.shipping_fee")).alias("shipping_fee_count"),
    )
    .orderBy("city")
)
