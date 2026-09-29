from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("launch_date") >= "2025-06-21").alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.rating") <= 2.0)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.country"))
    .orderBy(F.col("country").desc())
)
