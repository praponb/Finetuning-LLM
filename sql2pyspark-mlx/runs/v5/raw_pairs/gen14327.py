from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount")

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-01-21")
    .select("id", "merchant", "amount", F.dense_rank().over(w).alias("dense_rnk"))
)
