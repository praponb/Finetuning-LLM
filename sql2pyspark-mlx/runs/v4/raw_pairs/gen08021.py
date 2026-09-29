from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id") == 29)
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .orderBy("status")
)
