from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("S"), F.col("p.id") == F.col("S.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("S.units_count"),
    )
)
