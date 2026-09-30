from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)
w_running_2 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("delivered_date", "id")
w4 = Window.orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .withColumn("cost_avg_3", F.avg("cost").over(w_running))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w_running_2))
    .withColumn("prev_cost", F.lag("cost", 1).over(w3))
    .withColumn("cost_rank", F.rank().over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)
