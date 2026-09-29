from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.array_contains("tags", "bestseller")).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .filter(F.col("suppliers.country") == "DE")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
    )
)
