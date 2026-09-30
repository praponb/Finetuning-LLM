from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 400)
    .select(F.explode("tags").alias("label"))
    .groupBy("label")
    .agg(F.count("*").alias("product_count"))
)
