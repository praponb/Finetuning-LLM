from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time")

result = (
    df_transactions
    .select("id", "merchant", "txn_time", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "merchant", "txn_time")
)
