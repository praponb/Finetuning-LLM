from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.order_id").isin([7, 16, 25, 37]))
    .select(F.col("oi.quantity"), F.col("oi.unit_price"), F.col("p.category"))
    .orderBy(F.col("unit_price").desc())
)
