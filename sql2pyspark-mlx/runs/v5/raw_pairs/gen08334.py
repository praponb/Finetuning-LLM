from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("launch_date") <= "2025-05-15").alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .select(F.col("suppliers.id"), F.col("suppliers.city"), F.col("suppliers.country"))
)
