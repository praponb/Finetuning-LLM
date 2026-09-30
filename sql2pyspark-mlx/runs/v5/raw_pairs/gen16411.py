from pyspark.sql import functions as F

result = df_orders.agg(F.min("order_date").alias("first_order_date"))
