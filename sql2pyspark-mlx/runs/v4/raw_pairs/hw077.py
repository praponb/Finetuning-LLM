from pyspark.sql import functions as F

result = df_products.select("id", F.regexp_replace("product_name", "-", " ").alias("clean_name"))
