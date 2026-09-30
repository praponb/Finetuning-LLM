from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc(), "id")

result = df_order_items.select("id", "unit_price", F.row_number().over(w).alias("rn"))
