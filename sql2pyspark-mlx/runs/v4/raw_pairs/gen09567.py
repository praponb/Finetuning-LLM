from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("product_id") == 8)
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.count("*").alias("store_sale_count"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.store_sale_count") >= 4)
    .select(
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("t.unique_stores"),
        F.col("t.store_sale_count"),
    )
)
