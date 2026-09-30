from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status")
w2 = Window.partitionBy("status")

result = (
    df_payments
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_share_2", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("status_avg_amount", F.avg("amount").over(w2))
)
