from pyspark.sql import functions as F

result = df_products.select("id", F.array_contains("tags", "sale").alias("on_sale"))
