from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("carrier", "order_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "order_id")
w5 = Window.partitionBy("carrier", "order_id").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cost_avg_2", F.avg("cost").over(w2))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w3))
    .withColumn("status_max_cost", F.max("cost").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)
