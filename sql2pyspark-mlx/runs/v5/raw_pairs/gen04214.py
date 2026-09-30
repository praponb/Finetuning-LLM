from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .groupBy(F.col("products.brand"))
    .agg(
        F.count(F.col("store_sales.id")).alias("store_sale_count"),
        F.count(F.col("store_sales.units")).alias("units_count"),
        F.countDistinct(F.col("store_sales.units")).alias("unique_stores"),
    )
    .orderBy("units_count", "brand")
    .limit(5)
)
