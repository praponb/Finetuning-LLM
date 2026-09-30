from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3700)
    .groupBy("product_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
    .filter(F.col("avg_revenue") < 43)
    .select("product_id", "avg_revenue", "max_revenue")
    .orderBy(F.col("product_id").desc())
)
