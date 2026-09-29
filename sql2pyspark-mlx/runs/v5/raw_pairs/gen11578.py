from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("brand") == "Initech").alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .select(F.col("suppliers.id"), F.col("suppliers.rating"))
)
