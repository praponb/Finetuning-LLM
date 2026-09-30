from pyspark.sql import functions as F

result = df_products.select("category", F.sort_array("tags").alias("sorted_tags"))
