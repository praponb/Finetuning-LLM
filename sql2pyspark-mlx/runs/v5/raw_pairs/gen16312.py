from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")

result = (
    df_transactions
    .withColumn("max_amount_by_txn_type", F.max("amount").over(w))
    .filter(F.col("amount") == F.col("max_amount_by_txn_type"))
    .select("id", "txn_type", "amount", "account_id")
)
