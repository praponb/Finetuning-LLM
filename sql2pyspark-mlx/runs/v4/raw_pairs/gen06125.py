from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")
w2 = Window.orderBy("cost", "id").rowsBetween(-5, -1)

result = (
    df_shipments
    .withColumn("cost_rank", F.rank().over(w))
    .withColumn("cost_avg_5_prior", F.avg("cost").over(w2))
)
