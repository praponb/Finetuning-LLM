from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy(F.col("txn_time").desc(), "id")
w4 = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("merchant")

result = (
    df_transactions
    .withColumn("amount_avg_7d", F.avg("amount").over(w))
    .withColumn("amount_avg_3", F.avg("amount").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("amount_avg_2", F.avg("amount").over(w4))
    .withColumn("merchant_min_amount", F.min("amount").over(w_part))
)
