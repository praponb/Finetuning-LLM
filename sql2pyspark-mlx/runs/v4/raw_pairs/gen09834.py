from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("cost")

result = (
    df_shipments
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w))
    .withColumn("running_cost", F.sum("cost").over(w2))
    .withColumn("cost_dense_rank", F.dense_rank().over(w3))
)
