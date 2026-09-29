from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        "channel",
        F.date_trunc("MONTH", "order_date").alias("order_month"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") < 36)
)
