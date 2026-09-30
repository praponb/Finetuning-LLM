from pyspark.sql import functions as F

result = df_payments.select("status", F.length("status").alias("status_length"))
