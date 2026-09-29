from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy("cost", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .withColumn("cost_bucket", F.ntile(3).over(w))
    .withColumn("order_id_min_cost", F.min("cost").over(w2))
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w3))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w4))
    .withColumn("cumulative_cost", F.sum("cost").over(w5))
)
