from pyspark.sql import functions as F

result = (
    df_products
    .agg(F.min("price").alias("min_price"))
    .orderBy("min_price")
)
