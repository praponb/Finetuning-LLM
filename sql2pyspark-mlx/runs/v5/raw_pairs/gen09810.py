from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .select("id", "merchant", "txn_time", "account_id", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "merchant", "txn_time", "account_id")
)
