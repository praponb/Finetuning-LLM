from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.product_id").isNotNull())
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
)
