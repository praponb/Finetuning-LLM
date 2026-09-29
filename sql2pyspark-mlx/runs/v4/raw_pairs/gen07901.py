from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())

result = df_products.select("id", "category", "stock_qty", F.rank().over(w).alias("rnk"))
