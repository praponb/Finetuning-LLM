from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("stock_qty").desc())
)
