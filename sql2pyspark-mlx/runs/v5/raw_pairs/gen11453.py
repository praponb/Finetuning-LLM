from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.25, 0.39) & (F.col("channel") == "mobile"))
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("min_shipping_fee") < 19)
    .orderBy("order_quarter")
)
