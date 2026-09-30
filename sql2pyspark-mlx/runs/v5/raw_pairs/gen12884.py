from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6, 7, 9]))
    .groupBy(
        F.when(F.col("revenue") >= 3700, "high").when(F.col("revenue") >= 600, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.countDistinct("revenue").alias("unique_products"),
    )
    .filter(F.col("avg_revenue") > 38)
    .orderBy(F.col("revenue_bucket").desc())
    .limit(5)
)
