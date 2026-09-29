from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "prev_txn_time",
        "session_start_flag",
        "session_no",
        F.col("avg_transaction_amount"),
    )
    .orderBy(F.col("amount").desc())
)
