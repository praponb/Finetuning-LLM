from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cost_rank", F.dense_rank().over(w))
    .orderBy(F.col("cost_rank").desc())
)
