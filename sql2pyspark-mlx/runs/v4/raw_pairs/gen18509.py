from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web", "partner"]) | (F.col("discount") < 0.25))
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_pending"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
)
