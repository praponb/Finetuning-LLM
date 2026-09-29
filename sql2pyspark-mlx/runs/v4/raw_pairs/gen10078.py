from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 10]) & (F.col("product_id") == 5))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)

store_sales_stats = (
    src
    .groupBy("store_id", "product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    store_sales_stats
    .orderBy(F.col("store_id").desc(), "product_id")
    .limit(10)
)
