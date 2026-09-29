from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("merchant").isNull())
    .select("id", "txn_type", "amount", "txn_time", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "txn_type", "amount", "txn_time")
)
