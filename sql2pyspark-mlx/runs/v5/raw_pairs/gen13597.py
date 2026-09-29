from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("product_id") == 1)
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)

result = (
    df_products.alias("products")
    .join(store_sales_by_product.alias("store_sales_by_product"), F.col("products.id") == F.col("store_sales_by_product.product_id"), "inner")
    .filter(F.col("store_sales_by_product.unique_stores") >= 6)
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.supplier_id"),
        F.col("store_sales_by_product.unique_stores"),
    )
    .orderBy(F.col("unique_stores").desc())
)
