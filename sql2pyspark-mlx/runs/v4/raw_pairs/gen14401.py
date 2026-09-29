from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") <= 2.8)
    .select(
        F.col("s.city"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("p.price"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("rating").desc())
)
