from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter((F.col("store_id") == 35) & F.col("customer_id").isin([4, 8, 21, 36]))
)

channel_summary = (
    filtered_orders
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("completed_amount"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
)

result = channel_summary.select("channel", "completed_amount", "avg_shipping_fee")
