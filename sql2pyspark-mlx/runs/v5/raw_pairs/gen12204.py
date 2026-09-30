from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(-5, -1)

result = (
    df_shipments
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cost_avg_5", F.avg("cost").over(w2))
    .withColumn("cost_avg_5_prior", F.avg("cost").over(w_running))
)
