from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .filter(F.col("price") < 1090)
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "category", "stock_qty")
)
