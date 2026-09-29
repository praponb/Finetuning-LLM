from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost")

result = (
    df_products
    .filter(F.size("tags") > 1)
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy("brand")
)
