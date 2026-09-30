from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("store_sales.revenue") < 1700)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
        F.col("products.price"),
        F.col("products.supplier_id"),
        F.col("suppliers.city"),
    )
    .orderBy("store_id", "id")
    .limit(3)
)
