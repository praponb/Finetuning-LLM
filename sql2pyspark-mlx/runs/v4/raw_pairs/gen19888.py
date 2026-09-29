from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter((F.col("p.price") > 1120) & F.array_contains(F.col("p.tags"), "bestseller"))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("quantity").desc())
)
