from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("P.avg_revenue"),
        F.col("P.cnt"),
    )
    .orderBy(F.col("avg_revenue").desc(), "price")
)
