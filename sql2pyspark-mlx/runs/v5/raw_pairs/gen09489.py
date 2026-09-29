from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & (F.col("store_id") == 12))
    .groupBy("status")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
