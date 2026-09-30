from pyspark.sql import functions as F

result = df_order_items.select("id", (F.col("quantity") * F.col("unit_price")).alias("line_total"))
