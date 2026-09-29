from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "eco"))
    .select(
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.rating"),
    )
)
