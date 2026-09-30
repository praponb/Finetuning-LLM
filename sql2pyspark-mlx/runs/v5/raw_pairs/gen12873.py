from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")

result = (
    df_transactions
    .withColumn("min_amount_by_txn_type", F.min("amount").over(w))
    .filter(F.col("amount") == F.col("min_amount_by_txn_type"))
    .select("id", "txn_type", "amount", "txn_time")
    .orderBy("txn_time", F.col("amount").desc())
)
