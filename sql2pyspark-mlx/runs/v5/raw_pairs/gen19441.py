from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating") > 3.6)
    .groupBy(F.col("s.country"))
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .orderBy(F.col("country").desc())
    .limit(3)
)
