from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"), "status")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy("stddev_discount", "order_month", "status")
    .limit(50)
)
