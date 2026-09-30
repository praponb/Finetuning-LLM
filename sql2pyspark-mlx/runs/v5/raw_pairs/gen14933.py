from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .orderBy("method")
)
