from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.count("price").alias("price_count"))
)
