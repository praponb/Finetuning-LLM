from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.channel").isin(["store", "web", "mobile"]))
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc(), "order_month")
    .limit(3)
)
