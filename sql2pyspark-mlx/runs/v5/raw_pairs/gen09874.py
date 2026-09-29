from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4, 6]))
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.revenue_count") >= 7)
    .select(
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("ps.last_sale_date"),
        F.col("ps.revenue_count"),
    )
    .orderBy(F.col("product_name").desc())
)
