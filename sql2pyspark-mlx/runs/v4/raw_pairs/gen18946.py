from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 29) | (F.col("status") == "COMPLETED"))
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.min("order_date").alias("min_order_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 5)
    .drop("total_count")
)
