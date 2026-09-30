from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id").isin([13, 38]))
    .select(
        F.col("p.brand"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
    .orderBy(F.col("country").desc())
)
