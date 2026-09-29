from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") != "web") & ((F.col("shipping_fee") < 14) | (F.col("shipping_fee") < 16)))
    .groupBy("channel")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)
