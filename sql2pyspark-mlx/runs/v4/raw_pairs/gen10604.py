from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter((F.col("suppliers.country") == "US") & F.col("products.price").isNull())
    .select(
        F.col("store_sales.revenue"),
        F.col("store_sales.store_id"),
        F.col("products.category"),
        F.col("suppliers.supplier_name"),
        F.lower(F.col("suppliers.city")).alias("city_lower"),
    )
)
