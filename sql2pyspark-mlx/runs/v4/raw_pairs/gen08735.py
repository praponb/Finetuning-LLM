from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("shipping_fee") < 30)

orders_stats = (
    recent_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
    )
)

result = orders_stats.orderBy("web_count")
