from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date")

result = (
    df_shipments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "order_id", "delivered_date")
    .orderBy("delivered_date", F.col("id").desc())
)
