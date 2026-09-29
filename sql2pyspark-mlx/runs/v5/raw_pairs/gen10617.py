from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.launch_date"),
        F.col("t.min_sale_date"),
    )
    .orderBy("launch_date")
)
