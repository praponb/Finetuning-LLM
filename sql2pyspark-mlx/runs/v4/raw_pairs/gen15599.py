from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "premium"))
    .select(
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
)
