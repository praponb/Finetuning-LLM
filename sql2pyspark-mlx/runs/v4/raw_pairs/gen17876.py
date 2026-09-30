from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("price") <= 1100).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_anti")
    .select(F.col("suppliers.id"), F.col("suppliers.country"))
    .orderBy("country", F.col("id").desc())
)
