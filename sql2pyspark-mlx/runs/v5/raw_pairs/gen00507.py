from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date")

result = (
    df_shipments
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") == 1)
    .orderBy("order_id", "status")
)
