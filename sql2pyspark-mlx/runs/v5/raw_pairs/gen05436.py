from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.unique_products") > 7)
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("t.first_sale_date"),
        F.col("t.unique_products"),
    )
)
