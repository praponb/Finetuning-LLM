from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .select("id", "account_id", "txn_time", F.rank().over(w).alias("rnk"))
)
