from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
