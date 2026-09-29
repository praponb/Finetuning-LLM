from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "prev_txn_time",
        "session_no",
        F.when(F.col("txn_type").rlike("^(FEE|DEBIT)$"), "GROUP_B").when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("txn_type_zone"),
        F.col("min_transaction_amount"),
    )
    .orderBy(F.col("amount").asc_nulls_first())
)
