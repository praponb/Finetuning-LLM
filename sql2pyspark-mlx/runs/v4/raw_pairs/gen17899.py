from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("city") != "Singapore").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.product_name").like("Lap%"))
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.category"))
    .orderBy(F.col("category").desc())
)
