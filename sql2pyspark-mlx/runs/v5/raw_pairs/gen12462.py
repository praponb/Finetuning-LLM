from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.product_id").isin([8, 15]) | F.col("p.stock_qty").isNotNull())
    .select(
        F.col("p.price"),
        F.col("p.brand"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
    .orderBy(F.col("quantity").desc())
)
