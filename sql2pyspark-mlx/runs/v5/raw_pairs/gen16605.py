from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.product_name").isNotNull()), "left_anti")
)

result = (
    kept
    .select("id", "supplier_name", "country", "city")
    .orderBy(F.col("country").desc_nulls_last())
)
