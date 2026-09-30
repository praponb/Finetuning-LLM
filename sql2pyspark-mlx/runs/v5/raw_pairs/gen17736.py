from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isNull()), "left_semi")
)

result = (
    kept
    .select("id", "country", "rating")
    .orderBy(F.col("country").desc())
)
