from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .withColumn("prev_cost", F.lag("cost", 2).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)
