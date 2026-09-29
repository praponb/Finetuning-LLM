from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .select(
        F.col("store_sales.units"),
        F.col("products.supplier_id"),
        F.col("suppliers.rating"),
        F.col("suppliers.country"),
    )
)
