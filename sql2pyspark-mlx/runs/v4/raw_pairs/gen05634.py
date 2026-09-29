from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_products
    .filter(F.col("price") > 580)
    .groupBy("category")
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("rnk", F.dense_rank().over(w))
)
