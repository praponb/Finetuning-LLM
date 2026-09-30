from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.col("stock_qty") > 369).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.unit_price").between(120, 160))
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("oi.product_id"))
)
