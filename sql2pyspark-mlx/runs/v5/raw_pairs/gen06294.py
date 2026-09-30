from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.order_id").isin([11, 12]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("p.category"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(5)
)
