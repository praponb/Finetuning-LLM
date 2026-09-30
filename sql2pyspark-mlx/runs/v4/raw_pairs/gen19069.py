from pyspark.sql import functions as F

result = df_orders.filter((F.col("amount") <= 900) & F.col("discount").between(0.28, 0.38))
