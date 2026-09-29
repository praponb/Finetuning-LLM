from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("order_date") >= "2024-02-06")
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy("store_id", "row_num")
)
