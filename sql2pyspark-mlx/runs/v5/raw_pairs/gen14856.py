from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("discount") >= 0.25)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.concat_ws("/", "channel", "status").alias("channel_status_key"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("channel") == "store", F.col("shipping_fee")).otherwise(0)).alias("store_shipping_fee"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
    )
)
