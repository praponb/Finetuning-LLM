from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.city") == "Berlin")
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("p.category"),
    )
)
