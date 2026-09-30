from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w3 = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w4 = Window.partitionBy("carrier", "order_id")

result = (
    df_shipments
    .withColumn("status_min_cost", F.min("cost").over(w))
    .withColumn("prev_cost", F.lag("cost", 2).over(w2))
    .withColumn("prev_cost_2", F.lag("cost", 1).over(w3))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
)
