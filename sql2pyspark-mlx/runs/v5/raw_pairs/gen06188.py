from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("id").desc())
)
