from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")
w2 = Window.orderBy(F.col("cost").desc())

with_windows = (
    df_shipments
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("cost_dense_rank", F.dense_rank().over(w2))
)

result = (
    with_windows
    .select("id", "carrier", "delivered_date", "cost", "status", "cost_dense_rank")
    .orderBy(F.col("carrier").desc())
)
