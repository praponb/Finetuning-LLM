from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("ssbp"), F.col("p.id") == F.col("ssbp.product_id"), "left")
    .filter(F.col("ssbp.avg_revenue") >= 3600)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("ssbp.min_revenue"),
        F.col("ssbp.avg_revenue"),
    )
)
