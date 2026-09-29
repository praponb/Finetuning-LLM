from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .groupBy(F.col("suppliers.country"))
    .agg(
        F.count(F.col("store_sales.revenue")).alias("revenue_count"),
        F.countDistinct(F.col("store_sales.revenue")).alias("unique_stores"),
    )
    .orderBy("country", F.col("revenue_count").desc())
)
