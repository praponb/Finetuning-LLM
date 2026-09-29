from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .groupBy(
        "channel",
        F.when(F.col("status").isin(["CANCELLED", "RETURNED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("stddev_discount") > 58)
    .select("channel", "status_group", "stddev_discount", "last_order_date")
)
