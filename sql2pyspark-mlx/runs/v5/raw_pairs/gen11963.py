from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("discount").alias("discount_count"),
        F.max("order_date").alias("last_order_date"),
    )
)
