from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("stock_qty").desc())

result = df_products.select("id", "stock_qty", F.rank().over(w).alias("stock_qty_rank"))
