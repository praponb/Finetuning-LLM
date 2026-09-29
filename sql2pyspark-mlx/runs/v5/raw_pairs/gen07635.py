from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .select(
        "id",
        "txn_type",
        "amount",
        "account_id",
        "txn_time",
        F.rank().over(w).alias("amount_rank"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
    )
)
