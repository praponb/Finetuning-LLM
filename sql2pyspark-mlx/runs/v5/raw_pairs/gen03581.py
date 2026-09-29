from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("customer_id").isNotNull())

orders_stats = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = orders_stats
