from pyspark.sql import functions as F

result = df_order_items.agg(F.max("quantity").alias("max_quantity"))
