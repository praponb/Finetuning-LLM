from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 37)
    .groupBy("status", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("shipping_fee").alias("shipping_fee_count"),
    )
    .filter(F.col("shipping_fee_count") < 4)
    .orderBy("shipping_fee_count", "status", "order_quarter")
    .limit(10)
)
