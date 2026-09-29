from pyspark.sql import functions as F

result = (
    df_products
    .select(F.explode("tags").alias("tag_name"))
    .groupBy("tag_name")
    .agg(F.count("*").alias("product_count"))
)
