from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("oi.product_id").isNotNull())
    .groupBy(F.col("o.status"))
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
    )
    .filter(F.col("revenue") >= 57)
)
