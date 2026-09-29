from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand").isNull()), "left_semi")
)

result = (
    kept
    .select("id", "supplier_name", "city", "rating")
    .orderBy(F.col("rating").desc())
)
