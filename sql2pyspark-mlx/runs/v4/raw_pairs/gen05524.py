from pyspark.sql import functions as F

result = df_products.withColumn("is_new", F.array_contains("tags", "new"))
