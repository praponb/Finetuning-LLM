from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time")

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "REFUND"]))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        "account_id",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy(F.col("dense_rnk").desc())
)
