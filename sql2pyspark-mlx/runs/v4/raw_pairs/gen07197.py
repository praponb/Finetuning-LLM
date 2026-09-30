from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("cost", "id")
w2 = Window.orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("carrier").orderBy("cost")
w4 = Window.partitionBy("status")
w5 = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .withColumn("cost_bucket", F.ntile(10).over(w_hist))
    .withColumn("cost_dense_rank", F.dense_rank().over(w2))
    .withColumn("cost_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
    .withColumn("row_seq", F.row_number().over(w5))
)
