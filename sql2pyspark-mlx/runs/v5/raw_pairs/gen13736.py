from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.size(F.col("p.tags")) >= 2)
    .select(F.col("s.city"), F.col("p.category"), F.col("p.product_name"))
    .orderBy(F.col("category").desc())
)
