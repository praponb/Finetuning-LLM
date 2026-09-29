from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2024-09-01")
    .select(F.explode("tags").alias("tag"))
    .groupBy("tag")
    .agg(F.count("*").alias("product_count"))
    .orderBy(F.col("product_count").desc(), "tag")
)
