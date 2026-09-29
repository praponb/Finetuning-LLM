from pyspark.sql import functions as F

result = df_sessions.select("customer_id", F.length("country").alias("country_length"))
