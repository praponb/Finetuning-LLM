from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
    .filter(F.col("avg_revenue") > 1600)
    .orderBy(F.col("product_id").desc(), "store_id")
    .limit(5)
)
