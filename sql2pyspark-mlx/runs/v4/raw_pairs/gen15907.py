from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([6, 21, 25, 28]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .orderBy("method", "pos")
)
