from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_revenue") > 2900)
    .select(
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("t.last_sale_date"),
        F.col("t.avg_revenue"),
    )
)
