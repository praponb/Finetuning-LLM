from pyspark.sql import functions as F

result = df_order_items.filter(F.col("unit_price") < 160)
