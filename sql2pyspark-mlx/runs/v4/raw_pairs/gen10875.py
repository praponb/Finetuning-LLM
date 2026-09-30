from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("suppliers.country") == "SG")
    .select(
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
        F.col("products.product_name"),
        F.col("products.category"),
        F.col("products.supplier_id"),
    )
    .orderBy(F.col("rating").desc())
)
