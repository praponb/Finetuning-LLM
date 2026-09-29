from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)
