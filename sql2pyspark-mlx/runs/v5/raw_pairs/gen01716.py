from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("customer_id") == 26)

orders_stats = (
    filtered_orders
    .groupBy("channel")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = orders_stats.orderBy("channel")
