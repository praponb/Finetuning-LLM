from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(F.col("p.supplier_id"), F.col("i.quantity"), F.col("i.store_id"))
    .orderBy(F.col("store_id").desc())
)
