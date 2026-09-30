from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-5, -1)

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 12))
    .withColumn("amount_count_30d", F.count("amount").over(w))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("prev_amount", F.lag("amount", 2).over(w3))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w4))
)
