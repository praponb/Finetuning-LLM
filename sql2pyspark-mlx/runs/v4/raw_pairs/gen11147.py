from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "inner")
    .select(
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("ssbp.avg_revenue"),
        F.col("ssbp.total_count"),
    )
    .orderBy("category")
)
