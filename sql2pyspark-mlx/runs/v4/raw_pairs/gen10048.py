from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .orderBy("city", "max_quantity")
)
