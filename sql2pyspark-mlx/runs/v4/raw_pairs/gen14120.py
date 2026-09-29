from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("channel").desc(), "first_order_date")
)
