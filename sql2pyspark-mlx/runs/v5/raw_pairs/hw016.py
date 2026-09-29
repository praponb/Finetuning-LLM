from pyspark.sql import functions as F

result = df_products.select("id", F.ceil("price").alias("price_ceil"), F.floor("price").alias("price_floor"))
