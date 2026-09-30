from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.min("price").alias("min_price"), F.max("price").alias("max_price"))
)
