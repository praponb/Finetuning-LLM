from pyspark.sql import functions as F

result = df_customers.agg(F.count("*").alias("total_count"))
