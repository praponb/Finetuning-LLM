from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .select(
        F.col("suppliers.country"),
        F.col("suppliers.supplier_name"),
        F.col("products.cost"),
        F.array_contains(F.col("products.tags"), "eco").alias("is_eco"),
    )
    .orderBy(F.col("supplier_name").desc())
)
