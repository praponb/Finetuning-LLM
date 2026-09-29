from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([18, 30, 33]))
    .select("id", "txn_time", "amount", F.first("amount").over(w).alias("first_amount"))
)
