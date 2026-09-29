from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.store_id").isin([2, 3, 4, 6]))
    .groupBy(F.col("s.country"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
)
