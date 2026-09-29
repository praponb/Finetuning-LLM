from pyspark.sql import functions as F

result = df_orders.cube("channel", "status").agg(F.count("*").alias("n"))
