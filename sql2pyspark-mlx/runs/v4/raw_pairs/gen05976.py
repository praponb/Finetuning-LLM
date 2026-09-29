from pyspark.sql import functions as F

result = df_orders.withColumn("status_length", F.length("status"))
