from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("country") == "UK").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.launch_date").between("2023-04-01", "2025-04-01"))
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.brand"))
)
