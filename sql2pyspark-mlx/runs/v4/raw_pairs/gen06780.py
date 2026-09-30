from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").desc())
w3 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .withColumn("running_cost", F.sum("cost").over(w))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("running_cost_2", F.sum("cost").over(w3))
)
