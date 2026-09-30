from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .withColumn("next_cost", F.lead("cost").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("status_sum_cost", F.sum("cost").over(w2))
)
