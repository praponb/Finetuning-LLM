from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
    .filter(F.col("shipping_fee_range") < 57)
)
