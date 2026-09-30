from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee")

result = df_orders.select("id", "shipping_fee", F.rank().over(w).alias("shipping_fee_rank"))
