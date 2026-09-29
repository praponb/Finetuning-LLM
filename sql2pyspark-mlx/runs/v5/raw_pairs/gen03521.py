from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.product_name"),
        F.col("products.category"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
    )
)
