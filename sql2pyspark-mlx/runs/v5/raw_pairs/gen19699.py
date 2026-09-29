from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.lower(F.trim("supplier_name")) == "lion city imports").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.product_name"))
    .orderBy(F.col("product_name").desc())
)
