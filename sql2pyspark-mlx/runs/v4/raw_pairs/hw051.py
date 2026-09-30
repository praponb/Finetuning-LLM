from pyspark.sql import functions as F

result = df_customers.select("id", F.when(F.col("age") >= 30, "Senior").otherwise("Junior").alias("age_group"))
