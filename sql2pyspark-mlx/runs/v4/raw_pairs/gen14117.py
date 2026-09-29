from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.region") != "West")
    .select(F.col("i.store_id"), F.col("s.city"), F.col("p.supplier_id"))
    .orderBy("supplier_id")
)
