from pyspark.sql import functions as F

result = df_orders.orderBy(F.col("amount").desc(), F.col("order_date").asc())
