from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "store_id",
        F.date_trunc("quarter", "order_date").alias("quarter_start"),
        F.date_trunc("month", "order_date").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("shipping_fee_per_completed"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
)
