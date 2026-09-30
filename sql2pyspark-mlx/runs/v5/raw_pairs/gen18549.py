from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("amount").between(2900, 3500))
    .select("id", "txn_time", F.rank().over(w).alias("rnk"))
)
