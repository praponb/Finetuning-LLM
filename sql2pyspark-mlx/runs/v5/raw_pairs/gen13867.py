from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.lower(F.col("s.store_name")).like("%irp%") | (F.col("p.supplier_id") == 39))
    .select(F.col("i.id"), F.col("i.last_restock"), F.col("p.price"), F.col("s.region"))
    .orderBy(F.col("price").desc(), "id")
)
