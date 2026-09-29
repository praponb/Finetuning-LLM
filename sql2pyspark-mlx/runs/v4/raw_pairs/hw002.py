from pyspark.sql import functions as F

result = df_customers.select(F.col("id").alias("customer_id"), F.col("name").alias("customer_name"))
