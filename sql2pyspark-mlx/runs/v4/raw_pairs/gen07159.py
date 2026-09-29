from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.store_id").isin([1, 6]))
    .groupBy(F.col("p.supplier_id"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_products"))
)
