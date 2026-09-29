from pyspark.sql import functions as F

result = df_orders.filter((F.col("channel") == "web") | (F.col("channel") == "mobile"))
