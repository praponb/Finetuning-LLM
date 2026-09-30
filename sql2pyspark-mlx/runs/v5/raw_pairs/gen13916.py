from pyspark.sql import functions as F

result = df_order_items.withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
