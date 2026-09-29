from pyspark.sql import functions as F

result = df_products.select("id", "brand", "category", F.floor("cost").alias("cost_whole"))
