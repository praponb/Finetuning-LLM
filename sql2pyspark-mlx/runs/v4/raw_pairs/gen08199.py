from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.month("order_date").alias("order_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
    .filter(F.col("store_count") < 26)
)
