from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.array_contains("tags", "eco")).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.city"))
)
