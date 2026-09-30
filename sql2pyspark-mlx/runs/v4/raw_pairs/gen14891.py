from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy("paid_at", F.col("order_id").desc())
)
