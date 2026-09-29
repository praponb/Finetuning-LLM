from pyspark.sql import functions as F

result = df_products.select("id", F.substring("product_name", 1, 4).alias("prefix"))
