from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull() & ((F.col("amount") < 470) | F.col("customer_id").isin([3, 30, 38])))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.min("order_date").alias("min_order_date"),
    )
    .orderBy("min_order_date")
)
