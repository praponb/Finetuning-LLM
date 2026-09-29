from pyspark.sql import functions as F

result = df_products.select("id", "cost", "tags", F.upper("brand").alias("brand_upper"))
