from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id") == 14).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.city") == "New York")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("s.country"))
)
