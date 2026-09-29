from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("launch_date").between("2024-03-01", "2025-04-01")).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .filter(F.col("suppliers.rating") >= 2.1)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
    )
    .orderBy("country")
)
