from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc())

result = df_products.select("id", "brand", "price", F.rank().over(w).alias("price_rank"))
