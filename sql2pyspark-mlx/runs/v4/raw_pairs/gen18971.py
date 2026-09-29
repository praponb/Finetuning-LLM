from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w_grp = Window.partitionBy("channel")

enriched = (
    df_orders
    .select(
        "*",
        F.count("shipping_fee").over(w).alias("status_count_shipping_fee"),
        F.max("amount").over(w_grp).alias("channel_max_amount"),
    )
)

result = (
    enriched
    .groupBy("channel", F.date_trunc("QUARTER", "order_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_rows"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum("status_count_shipping_fee").alias("total_status_count_shipping_fee"),
    )
    .filter(F.col("shipped_count") < 1)
)
