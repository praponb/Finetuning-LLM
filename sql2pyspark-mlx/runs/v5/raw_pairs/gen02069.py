from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("product_id") == 7) & (F.col("revenue") >= 2700)) | (F.col("revenue") > 3900))
    .groupBy("product_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
    .orderBy("product_id", F.col("avg_revenue").desc())
)
