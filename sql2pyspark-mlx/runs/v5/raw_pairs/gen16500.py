from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("supplier_name").isNotNull()).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.size(F.col("p.tags")) >= 1)
    .select(F.col("p.id"), F.col("p.product_name"))
    .orderBy("product_name")
)
