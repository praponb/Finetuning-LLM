from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.countDistinct("units").alias("unique_products"))
)

result = (
    df_products.alias("products")
    .join(store_sales_by_product.alias("store_sales_by_product"), F.col("products.id") == F.col("store_sales_by_product.product_id"), "inner")
    .select(
        F.col("products.category"),
        F.col("products.cost"),
        F.col("store_sales_by_product.unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
