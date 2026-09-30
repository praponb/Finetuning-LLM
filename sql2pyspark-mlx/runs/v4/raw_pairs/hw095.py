from pyspark.sql import functions as F

result = df_products.select("id", F.explode("tags").alias("tag"))
