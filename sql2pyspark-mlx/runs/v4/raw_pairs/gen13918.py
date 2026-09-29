from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .filter(F.array_contains(F.col("p.tags"), "premium") & (F.col("s.city") == "Tokyo"))
    .select(
        F.col("p.category"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
)
