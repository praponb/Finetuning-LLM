from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time")

result = (
    df_transactions
    .select("id", "account_id", "txn_time", "amount", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "account_id", "txn_time", "amount")
)
