from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.month(F.col("o.order_date")).alias("order_month"),
        F.when(F.col("o.shipping_fee") >= 30, "high").when(F.col("o.shipping_fee") >= 15, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .orderBy("shipping_fee_bucket")
)
