from pyspark.sql import functions as F

result = df_orders.orderBy(F.col("amount").desc()).limit(10)
