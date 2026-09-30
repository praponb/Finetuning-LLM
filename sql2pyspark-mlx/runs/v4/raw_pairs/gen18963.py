from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("account_id", "txn_type")
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .withColumn("amount_rank", F.rank().over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("txn_type_avg_amount", F.avg("amount").over(w3))
    .withColumn("prev_amount", F.lag("amount", 1).over(w4))
)
