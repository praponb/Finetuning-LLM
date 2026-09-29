from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "new") & F.col("oi.product_id").isin([3, 13, 36]))
    .select(F.col("oi.quantity"), F.col("p.category"), F.col("p.stock_qty"))
    .orderBy(F.col("stock_qty").desc())
)
