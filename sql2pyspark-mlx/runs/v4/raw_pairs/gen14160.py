from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty")

result = (
    df_products
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "brand", "stock_qty")
)
