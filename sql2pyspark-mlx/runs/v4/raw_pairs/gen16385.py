from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
    .orderBy("channel")
)
