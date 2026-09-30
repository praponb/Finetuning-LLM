from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id").isNull())
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("p.cost"),
        F.col("s.country"),
        F.col("s.rating"),
    )
    .orderBy(F.col("rating").desc())
)
