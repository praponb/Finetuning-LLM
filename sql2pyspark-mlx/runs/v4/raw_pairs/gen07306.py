from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("revenue").alias("max_revenue"))
    .filter(F.col("max_revenue") >= 1300)
    .select("product_id", "max_revenue")
)
