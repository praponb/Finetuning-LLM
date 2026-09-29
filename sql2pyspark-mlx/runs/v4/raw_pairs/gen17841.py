from pyspark.sql import functions as F

result = df_products.select("id", "tags", "cost", F.trim("category").alias("clean_category"))
