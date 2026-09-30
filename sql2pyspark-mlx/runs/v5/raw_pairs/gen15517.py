from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating") >= 2.8)
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("p.supplier_id"),
        F.coalesce(F.col("s.rating"), F.lit(0)).alias("rating_filled"),
    )
    .orderBy(F.col("supplier_id").desc())
)
