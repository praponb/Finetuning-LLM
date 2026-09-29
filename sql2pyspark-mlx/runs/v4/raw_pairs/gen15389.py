from pyspark.sql import functions as F

with_dims = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.order_date"), F.col("d.shipping_fee"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
    )
    .orderBy(F.col("quantity_bit_or").desc_nulls_first())
)
