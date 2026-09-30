from pyspark.sql import functions as F

result = df_products.select("id", "category", "brand", F.explode("tags").alias("label"))
