from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([24, 35]) & (F.col("customer_id") == 12))
    .groupBy("status")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)
