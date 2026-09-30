from pyspark.sql import functions as F

result = df_customers.select("id", F.col("age").cast("double").alias("age_double"))
