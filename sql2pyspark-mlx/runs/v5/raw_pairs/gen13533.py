from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.filter(F.col("country") == "SG").alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_semi")
    .select(F.col("products.id"), F.col("products.category"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
