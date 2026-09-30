from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("status") == "RETURNED") & (F.col("status") == "RETURNED")) | F.col("store_id").isNotNull())
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("stddev_shipping_fee") >= 72)
    .orderBy("order_quarter", "status_values")
)
