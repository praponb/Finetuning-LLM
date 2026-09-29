from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("discount").between(0.16, 0.33))

summary = (
    recent_orders
    .groupBy("channel", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    summary
    .select("channel", "status_values")
    .orderBy(F.col("status_values").desc(), "channel")
)
