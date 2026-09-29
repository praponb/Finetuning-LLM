from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.id"), F.col("customers.country"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.round(F.avg(F.col("orders.shipping_fee")), 2).alias("avg_shipping_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("orders.channel")))).alias("channel_values"),
    )
)
