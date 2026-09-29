from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
    .orderBy(F.col("total_revenue").desc())
)
