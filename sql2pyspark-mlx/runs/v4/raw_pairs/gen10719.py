from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("account_id", "merchant")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id")

result = (
    df_transactions
    .withColumn("amount_bucket", F.ntile(10).over(w))
    .withColumn("amount_rank", F.rank().over(w2))
    .withColumn("account_id_sum_amount", F.sum("amount").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("amount_bucket_2", F.ntile(3).over(w5))
)
