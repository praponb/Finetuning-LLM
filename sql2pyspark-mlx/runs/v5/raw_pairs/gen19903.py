from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 5, 9]))
    .groupBy("status")
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("status").desc())
)
